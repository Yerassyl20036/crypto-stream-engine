package domain

import (
	"context"
	"time"
)

// Tick represents a single market data point
type Tick struct {
	Exchange  string    `json:"exchange"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    float64   `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
}

// Reset clears the tick for reuse in sync.Pool
func (t *Tick) Reset() {
	t.Exchange = ""
	t.Symbol = ""
	t.Price = 0
	t.Volume = 0
	t.Timestamp = time.Time{}
}

// Alert represents a calculated anomaly
type Alert struct {
	Type      AlertType `json:"type"`
	Symbol    string    `json:"symbol"`
	Message   string    `json:"message"`
	Severity  string    `json:"severity"`
	Timestamp time.Time `json:"timestamp"`
	Data      any       `json:"data,omitempty"`
}

type AlertType string

const (
	AlertTypeSpread     AlertType = "SPREAD"
	AlertTypeVolatility AlertType = "VOLATILITY"
	AlertTypeVWAP       AlertType = "VWAP"
)

// AggregatedData represents processed market data
type AggregatedData struct {
	Symbol         string             `json:"symbol"`
	Prices         map[string]float64 `json:"prices"` // exchange -> price
	VWAP           float64            `json:"vwap"`
	Spread         float64            `json:"spread"`
	Volatility     float64            `json:"volatility"`
	LastUpdateTime time.Time          `json:"last_update"`
}

// Provider defines the interface for exchange connectors
type Provider interface {
	// Name returns the exchange name
	Name() string

	// Connect establishes the WebSocket connection
	Connect(ctx context.Context) error

	// Subscribe starts streaming ticks for given symbols
	Subscribe(symbols []string) (<-chan *Tick, <-chan error)

	// Close gracefully shuts down the connection
	Close() error
}
