package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/razedwell/crypto-stream-engine/internal/config"
	"github.com/razedwell/crypto-stream-engine/internal/domain"
	"github.com/razedwell/crypto-stream-engine/internal/metrics"
	"github.com/razedwell/crypto-stream-engine/internal/pipeline"
	"github.com/razedwell/crypto-stream-engine/internal/provider"
	"github.com/razedwell/crypto-stream-engine/internal/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting Crypto Stream Engine...")

	// Load configuration
	cfg := config.Load()

	// Create root context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create WebSocket hub for frontend communication
	hub := server.NewHub()
	go hub.Run(ctx)

	// Create worker pool
	workerPool := pipeline.NewWorkerPool(cfg)
	workerPool.Start(ctx)

	// Create providers
	providers := []domain.Provider{
		provider.NewBinanceProvider(),
		provider.NewCoinbaseProvider(),
		provider.NewBybitProvider(),
		provider.NewOKXProvider(),
		// provider.NewKrakenProvider(), // Disabled as per user request
	}

	// Connect all providers
	var wg sync.WaitGroup
	for _, p := range providers {
		if err := p.Connect(ctx); err != nil {
			log.Fatalf("Failed to connect to %s: %v", p.Name(), err)
		}
		metrics.IncrementWSConnections()

		// Subscribe to symbols
		tickCh, errCh := p.Subscribe(cfg.Symbols)

		// Fan-in: merge provider streams into worker pool
		wg.Add(1)
		go func(name string, ticks <-chan *domain.Tick, errs <-chan error) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case tick := <-ticks:
					// Forward to worker pool
					select {
					case workerPool.GetTickChannel() <- tick:
					case <-ctx.Done():
						return
					}
				case err := <-errs:
					log.Printf("[%s] Error: %v", name, err)
				}
			}
		}(p.Name(), tickCh, errCh)
	}

	// Alert broadcaster
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case alert := <-workerPool.GetAlertChannel():
				hub.BroadcastAlert(alert)
				metrics.IncrementAlertsGenerated(string(alert.Type), alert.Severity)
			}
		}
	}()

	// Aggregated data broadcaster
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case agg := <-workerPool.GetAggregatedChannel():
				hub.BroadcastAggregatedData(agg)
			}
		}
	}()

	// Start HTTP server
	httpServer := server.NewServer(cfg, hub)
	go func() {
		if err := httpServer.Start(ctx); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Println("All systems operational. Press Ctrl+C to shutdown.")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("\nShutdown signal received. Initiating graceful shutdown...")

	// Cancel context to stop all goroutines
	cancel()

	// Give providers time to close connections
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// Close all providers
	for _, p := range providers {
		if err := p.Close(); err != nil {
			log.Printf("Error closing %s: %v", p.Name(), err)
		}
		metrics.DecrementWSConnections()
	}

	// Wait for all goroutines with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		workerPool.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All goroutines stopped cleanly")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout exceeded, forcing exit")
	}

	log.Println("Crypto Stream Engine stopped")
}
