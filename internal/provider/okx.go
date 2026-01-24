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

const okxWSURL = "wss://ws.okx.com:8443/ws/v5/public"

// OKXProvider implements the Provider interface for OKX
type OKXProvider struct {
	name   string
	conn   *websocket.Conn
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	tickCh chan *domain.Tick
	errCh  chan error
}

// OKX trade message format
type okxMessage struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID    string `json:"instId"`  // Instrument ID (e.g., BTC-USDT)
		TradeID   string `json:"tradeId"` // Trade ID
		Price     string `json:"px"`      // Price
		Size      string `json:"sz"`      // Size
		Side      string `json:"side"`    // Side (buy/sell)
		Timestamp string `json:"ts"`      // Timestamp in milliseconds
	} `json:"data"`
}

func NewOKXProvider() *OKXProvider {
	return &OKXProvider{
		name:   "okx",
		tickCh: make(chan *domain.Tick, 1000),
		errCh:  make(chan error, 10),
	}
}

func (o *OKXProvider) Name() string {
	return o.name
}

func (o *OKXProvider) Connect(ctx context.Context) error {
	o.ctx, o.cancel = context.WithCancel(ctx)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, okxWSURL, nil)
	if err != nil {
		return fmt.Errorf("okx connect failed: %w", err)
	}

	o.mu.Lock()
	o.conn = conn
	o.mu.Unlock()

	log.Printf("[%s] Connected to WebSocket", o.name)
	return nil
}

func (o *OKXProvider) Subscribe(symbols []string) (<-chan *domain.Tick, <-chan error) {
	// Build subscription message
	// Format: {"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}
	args := make([]map[string]string, len(symbols))
	for i, symbol := range symbols {
		// Convert BTCUSDT to BTC-USDT
		instID := convertToOKXSymbol(symbol)
		args[i] = map[string]string{
			"channel": "trades",
			"instId":  instID,
		}
	}

	subMsg := map[string]any{
		"op":   "subscribe",
		"args": args,
	}

	o.mu.RLock()
	conn := o.conn
	o.mu.RUnlock()

	if err := conn.WriteJSON(subMsg); err != nil {
		o.errCh <- fmt.Errorf("okx subscribe failed: %w", err)
		return o.tickCh, o.errCh
	}

	log.Printf("[%s] Subscribed to %v", o.name, args)

	// Start reading messages
	go o.readLoop()

	return o.tickCh, o.errCh
}

func (o *OKXProvider) readLoop() {
	o.mu.RLock()
	conn := o.conn
	o.mu.RUnlock()

	for {
		select {
		case <-o.ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case o.errCh <- fmt.Errorf("okx read error: %w", err):
				case <-o.ctx.Done():
					return
				}
				return
			}

			o.handleMessage(message)
		}
	}
}

func (o *OKXProvider) handleMessage(data []byte) {
	var msg okxMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		// Skip non-trade messages
		return
	}

	// Check if it's a trades channel message
	if msg.Arg.Channel != "trades" {
		return
	}

	// Process each trade
	for _, trade := range msg.Data {
		price, err := strconv.ParseFloat(trade.Price, 64)
		if err != nil {
			log.Printf("[%s] Invalid price: %s", o.name, trade.Price)
			continue
		}

		volume, err := strconv.ParseFloat(trade.Size, 64)
		if err != nil {
			log.Printf("[%s] Invalid volume: %s", o.name, trade.Size)
			continue
		}

		// Parse timestamp (milliseconds)
		timestamp, err := strconv.ParseInt(trade.Timestamp, 10, 64)
		if err != nil {
			timestamp = time.Now().UnixMilli()
		}

		// Convert symbol back to standard format (BTC-USDT -> BTCUSDT)
		symbol := convertFromOKXSymbol(trade.InstID)

		// Acquire tick from pool
		tick := AcquireTick()
		tick.Exchange = o.name
		tick.Symbol = symbol
		tick.Price = price
		tick.Volume = volume
		tick.Timestamp = time.UnixMilli(timestamp)

		select {
		case o.tickCh <- tick:
		case <-o.ctx.Done():
			ReleaseTick(tick)
			return
		default:
			// Channel full - backpressure signal
			ReleaseTick(tick)
		}
	}
}

func (o *OKXProvider) Close() error {
	if o.cancel != nil {
		o.cancel()
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.conn != nil {
		// Send close message
		err := o.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		if err != nil {
			log.Printf("[%s] Error sending close: %v", o.name, err)
		}

		// Close the connection
		if err := o.conn.Close(); err != nil {
			return fmt.Errorf("okx close failed: %w", err)
		}

		log.Printf("[%s] Connection closed", o.name)
	}

	return nil
}

// convertToOKXSymbol converts BTCUSDT to BTC-USDT
func convertToOKXSymbol(symbol string) string {
	// BTCUSDT -> BTC-USDT
	if len(symbol) >= 6 {
		if strings.HasSuffix(symbol, "USDT") {
			base := symbol[:len(symbol)-4]
			return fmt.Sprintf("%s-USDT", base)
		}
		// Default: insert dash after first 3 chars
		return fmt.Sprintf("%s-%s", symbol[:3], symbol[3:])
	}
	return symbol
}

// convertFromOKXSymbol converts BTC-USDT to BTCUSDT
func convertFromOKXSymbol(instID string) string {
	// BTC-USDT -> BTCUSDT
	return strings.ToUpper(strings.ReplaceAll(instID, "-", ""))
}
