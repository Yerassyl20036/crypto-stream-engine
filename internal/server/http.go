package server

import (
	"context"
	"embed"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razedwell/crypto-stream-engine/internal/config"
)

//go:embed web/*
var webContent embed.FS

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for demo; restrict in production
	},
}

type Server struct {
	cfg    *config.Config
	hub    *Hub
	server *http.Server
}

func NewServer(cfg *config.Config, hub *Hub) *Server {
	return &Server{
		cfg: cfg,
		hub: hub,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Serve static files
	mux.Handle("/", http.FileServer(http.FS(webContent)))

	// WebSocket endpoint
	mux.HandleFunc("/ws", s.handleWebSocket)

	// Prometheus metrics
	mux.Handle("/metrics", promhttp.Handler())

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	s.server = &http.Server{
		Addr:         s.cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("HTTP server starting on %s", s.cfg.HTTPAddr)

	// Run server in goroutine
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return s.Shutdown()
}

func (s *Server) Shutdown() error {
	log.Println("Shutting down HTTP server...")

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	return s.server.Shutdown(ctx)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	client := &Client{
		hub:  s.hub,
		conn: conn,
		send: make(chan []byte, 256),
	}

	client.hub.register <- client

	// Start read and write pumps
	go client.writePump()
	go client.readPump()
}
