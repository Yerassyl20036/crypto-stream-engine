package pipeline

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/razedwell/crypto-stream-engine/internal/config"
	"github.com/razedwell/crypto-stream-engine/internal/domain"
	"github.com/razedwell/crypto-stream-engine/internal/metrics"
	"github.com/razedwell/crypto-stream-engine/internal/processor"
	"github.com/razedwell/crypto-stream-engine/internal/provider"
)

// Worker processes ticks and generates alerts
type Worker struct {
	id           int
	cfg          *config.Config
	tickCh       <-chan *domain.Tick
	alertCh      chan<- *domain.Alert
	aggregatedCh chan<- *domain.AggregatedData

	// Per-symbol ring buffers for calculations
	vwapBuffers       map[string]*processor.RingBuffer
	volatilityBuffers map[string]*processor.RingBuffer

	// Track prices per exchange for spread calculation
	latestPrices map[string]map[string]float64 // symbol -> exchange -> price

	// Track baseline volatility
	baselineVolatility map[string]float64

	mu sync.RWMutex
}

// WorkerPool manages a pool of workers
type WorkerPool struct {
	workers []*Worker
	tickCh  chan *domain.Tick
	alertCh chan *domain.Alert
	aggCh   chan *domain.AggregatedData
	wg      sync.WaitGroup
	cfg     *config.Config
}

func NewWorkerPool(cfg *config.Config) *WorkerPool {
	return &WorkerPool{
		tickCh:  make(chan *domain.Tick, cfg.ChannelBufferSize),
		alertCh: make(chan *domain.Alert, 1000),
		aggCh:   make(chan *domain.AggregatedData, 1000),
		cfg:     cfg,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	// Create workers
	for i := 0; i < wp.cfg.WorkerPoolSize; i++ {
		worker := &Worker{
			id:                 i,
			cfg:                wp.cfg,
			tickCh:             wp.tickCh,
			alertCh:            wp.alertCh,
			aggregatedCh:       wp.aggCh,
			vwapBuffers:        make(map[string]*processor.RingBuffer),
			volatilityBuffers:  make(map[string]*processor.RingBuffer),
			latestPrices:       make(map[string]map[string]float64),
			baselineVolatility: make(map[string]float64),
		}

		wp.workers = append(wp.workers, worker)
		wp.wg.Add(1)

		go worker.run(ctx, &wp.wg)
	}

	log.Printf("Started %d workers", wp.cfg.WorkerPoolSize)
	metrics.SetActiveWorkers(wp.cfg.WorkerPoolSize)
}

func (wp *WorkerPool) GetTickChannel() chan<- *domain.Tick {
	return wp.tickCh
}

func (wp *WorkerPool) GetAlertChannel() <-chan *domain.Alert {
	return wp.alertCh
}

func (wp *WorkerPool) GetAggregatedChannel() <-chan *domain.AggregatedData {
	return wp.aggCh
}

func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

func (w *Worker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Second) // Aggregate data every 5s
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", w.id)
			return

		case tick := <-w.tickCh:
			start := time.Now()
			w.processTick(tick)

			// Return tick to pool after processing
			provider.ReleaseTick(tick)

			// Record metrics
			metrics.RecordProcessingLatency(time.Since(start).Seconds())
			metrics.IncrementMessagesProcessed(tick.Exchange, tick.Symbol)

		case <-ticker.C:
			w.publishAggregatedData()
		}
	}
}

func (w *Worker) processTick(tick *domain.Tick) {
	w.mu.Lock()
	defer w.mu.Unlock()

	symbol := tick.Symbol

	// Initialize buffers if needed
	if _, exists := w.vwapBuffers[symbol]; !exists {
		w.vwapBuffers[symbol] = processor.NewRingBuffer(w.cfg.VWAPWindowSeconds)
		w.volatilityBuffers[symbol] = processor.NewRingBuffer(w.cfg.VolatilityWindowSeconds)
	}

	if _, exists := w.latestPrices[symbol]; !exists {
		w.latestPrices[symbol] = make(map[string]float64)
	}

	// Update price tracking
	w.latestPrices[symbol][tick.Exchange] = tick.Price

	// Add to buffers
	w.vwapBuffers[symbol].Add(tick.Price, tick.Volume, tick.Timestamp)
	w.volatilityBuffers[symbol].Add(tick.Price, tick.Volume, tick.Timestamp)

	// Calculate metrics and check for alerts
	w.checkSpreadAlert(symbol)
	w.checkVolatilityAlert(symbol)
}

func (w *Worker) checkSpreadAlert(symbol string) {
	prices := w.latestPrices[symbol]
	if len(prices) < 2 {
		return // Need at least 2 exchanges
	}

	// Find min and max prices
	var minPrice, maxPrice float64
	var minExchange, maxExchange string
	first := true

	for exchange, price := range prices {
		if first {
			minPrice = price
			maxPrice = price
			minExchange = exchange
			maxExchange = exchange
			first = false
			continue
		}

		if price < minPrice {
			minPrice = price
			minExchange = exchange
		}
		if price > maxPrice {
			maxPrice = price
			maxExchange = exchange
		}
	}

	// Calculate spread percentage
	spreadPercent := ((maxPrice - minPrice) / minPrice) * 100

	if spreadPercent > w.cfg.SpreadThresholdPercent {
		alert := &domain.Alert{
			Type:   domain.AlertTypeSpread,
			Symbol: symbol,
			Message: fmt.Sprintf("Large spread detected: %.2f%% between %s ($%.2f) and %s ($%.2f)",
				spreadPercent, minExchange, minPrice, maxExchange, maxPrice),
			Severity:  w.getSeverity(spreadPercent, w.cfg.SpreadThresholdPercent),
			Timestamp: time.Now(),
			Data: map[string]any{
				"spread_percent": spreadPercent,
				"min_exchange":   minExchange,
				"max_exchange":   maxExchange,
				"min_price":      minPrice,
				"max_price":      maxPrice,
			},
		}

		select {
		case w.alertCh <- alert:
		default:
			// Alert channel full, drop alert
		}
	}
}

func (w *Worker) checkVolatilityAlert(symbol string) {
	buf := w.volatilityBuffers[symbol]

	// Need enough data points
	if buf.Len() < w.cfg.VolatilityWindowSeconds {
		return
	}

	// Calculate current volatility
	variance := buf.StdDev()
	currentVolatility := math.Sqrt(variance)

	// Initialize baseline if needed
	if w.baselineVolatility[symbol] == 0 {
		w.baselineVolatility[symbol] = currentVolatility
		return
	}

	baseline := w.baselineVolatility[symbol]

	// Check if volatility spiked
	if currentVolatility > baseline*w.cfg.VolatilityMultiplier {
		alert := &domain.Alert{
			Type:   domain.AlertTypeVolatility,
			Symbol: symbol,
			Message: fmt.Sprintf("Extreme volatility spike: %.2fx baseline (current: %.2f, baseline: %.2f)",
				currentVolatility/baseline, currentVolatility, baseline),
			Severity:  "HIGH",
			Timestamp: time.Now(),
			Data: map[string]any{
				"current_volatility":  currentVolatility,
				"baseline_volatility": baseline,
				"multiplier":          currentVolatility / baseline,
			},
		}

		select {
		case w.alertCh <- alert:
		default:
		}
	}

	// Update baseline with exponential moving average
	alpha := 0.1
	w.baselineVolatility[symbol] = alpha*currentVolatility + (1-alpha)*baseline
}

func (w *Worker) publishAggregatedData() {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for symbol, buf := range w.vwapBuffers {
		if buf.Len() == 0 {
			continue
		}

		vwap := buf.VWAP()
		spread := w.calculateSpread(symbol)
		volatility := math.Sqrt(w.volatilityBuffers[symbol].StdDev())

		agg := &domain.AggregatedData{
			Symbol:         symbol,
			Prices:         w.copyPrices(symbol),
			VWAP:           vwap,
			Spread:         spread,
			Volatility:     volatility,
			LastUpdateTime: time.Now(),
		}

		select {
		case w.aggregatedCh <- agg:
		default:
		}
	}
}

func (w *Worker) calculateSpread(symbol string) float64 {
	prices := w.latestPrices[symbol]
	if len(prices) < 2 {
		return 0
	}

	var min, max float64
	first := true

	for _, price := range prices {
		if first {
			min = price
			max = price
			first = false
			continue
		}
		if price < min {
			min = price
		}
		if price > max {
			max = price
		}
	}

	return ((max - min) / min) * 100
}

func (w *Worker) copyPrices(symbol string) map[string]float64 {
	copy := make(map[string]float64)
	for exchange, price := range w.latestPrices[symbol] {
		copy[exchange] = price
	}
	return copy
}

func (w *Worker) getSeverity(value, threshold float64) string {
	ratio := value / threshold
	if ratio > 3 {
		return "CRITICAL"
	} else if ratio > 2 {
		return "HIGH"
	}
	return "MEDIUM"
}
