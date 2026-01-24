package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/razedwell/crypto-stream-engine/internal/domain"
)

const bybitWSURL = "wss://stream.bybit.com/v5/public/spot"

// BybitProvider implements the Provider interface for Bybit
type BybitProvider struct {
	name   string
	conn   *websocket.Conn
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	tickCh chan *domain.Tick
	errCh  chan error
}

// Bybit trade message format
type bybitMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Trades []struct {
			Timestamp string `json:"T"` // Trade time
			Symbol    string `json:"s"` // Symbol
			Side      string `json:"S"` // Side
			Volume    string `json:"v"` // Volume
			Price     string `json:"p"` // Price
			TradeID   string `json:"i"` // Trade ID
		} `json:"data"`
	} `json:"data"`
}

func NewBybitProvider() *BybitProvider {
	return &BybitProvider{
		name:   "bybit",
		tickCh: make(chan *domain.Tick, 1000),
		errCh:  make(chan error, 10),
	}
}

func (b *BybitProvider) Name() string {
	return b.name
}

func (b *BybitProvider) Connect(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, bybitWSURL, nil)
	if err != nil {
		return fmt.Errorf("bybit connect failed: %w", err)
	}

	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()

	log.Printf("[%s] Connected to WebSocket", b.name)
	return nil
}

func (b *BybitProvider) Subscribe(symbols []string) (<-chan *domain.Tick, <-chan error) {
	// Build subscription message
	// Format: {"op":"subscribe","args":["publicTrade.BTCUSDT","publicTrade.ETHUSDT"]}
	args := make([]string, len(symbols))
	for i, symbol := range symbols {
		// Bybit uses uppercase symbols
		args[i] = fmt.Sprintf("publicTrade.%s", strings.ToUpper(symbol))
	}

	subMsg := map[string]any{
		"op":   "subscribe",
		"args": args,
	}

	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()

	if err := conn.WriteJSON(subMsg); err != nil {
		b.errCh <- fmt.Errorf("bybit subscribe failed: %w", err)
		return b.tickCh, b.errCh
	}

	log.Printf("[%s] Subscribed to %v", b.name, args)

	// Start reading messages
	go b.readLoop()

	return b.tickCh, b.errCh
}

func (b *BybitProvider) readLoop() {
	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case b.errCh <- fmt.Errorf("bybit read error: %w", err):
				case <-b.ctx.Done():
					return
				}
				return
			}

			b.handleMessage(message)
		}
	}
}

func (b *BybitProvider) handleMessage(data []byte) {
	var msg bybitMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		// Skip non-trade messages (pings, subscription confirmations, etc.)
		return
	}

	// Check if it's a trade message
	if !strings.HasPrefix(msg.Topic, "publicTrade.") {
		return
	}

	// Process each trade
	for _, trade := range msg.Data.Trades {
		price, err := strconv.ParseFloat(trade.Price, 64)
		if err != nil {
			log.Printf("[%s] Invalid price: %s", b.name, trade.Price)
			continue
		}

		volume, err := strconv.ParseFloat(trade.Volume, 64)
		if err != nil {
			log.Printf("[%s] Invalid volume: %s", b.name, trade.Volume)
			continue
		}

		// Parse timestamp (milliseconds)
		timestamp, err := strconv.ParseInt(trade.Timestamp, 10, 64)
		if err != nil {
			timestamp = time.Now().UnixMilli()
		}

		// Acquire tick from pool
		tick := AcquireTick()
		tick.Exchange = b.name
		tick.Symbol = strings.ToUpper(trade.Symbol)
		tick.Price = price
		tick.Volume = volume
		tick.Timestamp = time.UnixMilli(timestamp)

		select {
		case b.tickCh <- tick:
		case <-b.ctx.Done():
			ReleaseTick(tick)
			return
		default:
			// Channel full - backpressure signal
			ReleaseTick(tick)
		}
	}
}

func (b *BybitProvider) Close() error {
	if b.cancel != nil {
		b.cancel()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		// Send close message
		err := b.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		if err != nil {
			log.Printf("[%s] Error sending close: %v", b.name, err)
		}

		// Close the connection
		if err := b.conn.Close(); err != nil {
			return fmt.Errorf("bybit close failed: %w", err)
		}

		log.Printf("[%s] Connection closed", b.name)
	}

	return nil
}
