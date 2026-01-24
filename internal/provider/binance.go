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

const binanceWSURL = "wss://stream.binance.com:9443/ws"

// BinanceProvider implements the Provider interface for Binance
type BinanceProvider struct {
	name   string
	conn   *websocket.Conn
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	tickCh chan *domain.Tick
	errCh  chan error
}

// Binance aggregate trade message format
type binanceAggTrade struct {
	Event     string `json:"e"` // Event type
	EventTime int64  `json:"E"` // Event time
	Symbol    string `json:"s"` // Symbol
	Price     string `json:"p"` // Price
	Quantity  string `json:"q"` // Quantity
	TradeTime int64  `json:"T"` // Trade time
}

func NewBinanceProvider() *BinanceProvider {
	return &BinanceProvider{
		name:   "binance",
		tickCh: make(chan *domain.Tick, 1000),
		errCh:  make(chan error, 10),
	}
}

func (b *BinanceProvider) Name() string {
	return b.name
}

func (b *BinanceProvider) Connect(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, binanceWSURL, nil)
	if err != nil {
		return fmt.Errorf("binance connect failed: %w", err)
	}

	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()

	log.Printf("[%s] Connected to WebSocket", b.name)
	return nil
}

func (b *BinanceProvider) Subscribe(symbols []string) (<-chan *domain.Tick, <-chan error) {
	// Build subscription message
	// Format: {"method":"SUBSCRIBE","params":["btcusdt@aggTrade","ethusdt@aggTrade"],"id":1}
	streams := make([]string, len(symbols))
	for i, symbol := range symbols {
		// Binance requires lowercase symbols
		streams[i] = fmt.Sprintf("%s@aggTrade", strings.ToLower(symbol))
	}

	subMsg := map[string]any{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     1,
	}

	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()

	if err := conn.WriteJSON(subMsg); err != nil {
		b.errCh <- fmt.Errorf("binance subscribe failed: %w", err)
		return b.tickCh, b.errCh
	}

	log.Printf("[%s] Subscribed to %v", b.name, symbols)

	// Log subscription confirmation
	go func() {
		time.Sleep(2 * time.Second)
		log.Printf("[%s] Waiting for trade data on streams: %v", b.name, streams)
	}()

	// Start reading messages
	go b.readLoop()

	return b.tickCh, b.errCh
}

func (b *BinanceProvider) readLoop() {
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
				case b.errCh <- fmt.Errorf("binance read error: %w", err):
				case <-b.ctx.Done():
					return
				}
				return
			}

			b.handleMessage(message)
		}
	}
}

func (b *BinanceProvider) handleMessage(data []byte) {
	var trade binanceAggTrade
	if err := json.Unmarshal(data, &trade); err != nil {
		// Log first few non-parseable messages for debugging
		if len(data) < 200 {
			log.Printf("[%s] Non-trade message: %s", b.name, string(data))
		}
		return
	}

	// Only process aggTrade events
	if trade.Event != "aggTrade" {
		log.Printf("[%s] Skipping non-aggTrade event: %s", b.name, trade.Event)
		return
	}

	// log.Printf("[%s] Received trade: %s @ %s", b.name, trade.Symbol, trade.Price)

	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		log.Printf("[%s] Invalid price: %s", b.name, trade.Price)
		return
	}

	volume, err := strconv.ParseFloat(trade.Quantity, 64)
	if err != nil {
		log.Printf("[%s] Invalid volume: %s", b.name, trade.Quantity)
		return
	}

	// Acquire tick from pool
	tick := AcquireTick()
	tick.Exchange = b.name
	tick.Symbol = strings.ToUpper(trade.Symbol) // Normalize to uppercase
	tick.Price = price
	tick.Volume = volume
	tick.Timestamp = time.UnixMilli(trade.TradeTime)

	select {
	case b.tickCh <- tick:
	case <-b.ctx.Done():
		ReleaseTick(tick) // Don't leak if context cancelled
		return
	default:
		// Channel full - this is a backpressure signal
		// In production, you'd increment a dropped_messages metric
		ReleaseTick(tick)
	}
}

func (b *BinanceProvider) Close() error {
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
			return fmt.Errorf("binance close failed: %w", err)
		}

		log.Printf("[%s] Connection closed", b.name)
	}

	return nil
}
