package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/razedwell/crypto-stream-engine/internal/domain"
)

const coinbaseWSURL = "wss://ws-feed.exchange.coinbase.com"

type CoinbaseProvider struct {
	name   string
	conn   *websocket.Conn
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	tickCh chan *domain.Tick
	errCh  chan error
}

// Coinbase ticker message
type coinbaseTicker struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Volume24h string `json:"volume_24h"`
	Time      string `json:"time"`
}

func NewCoinbaseProvider() *CoinbaseProvider {
	return &CoinbaseProvider{
		name:   "coinbase",
		tickCh: make(chan *domain.Tick, 1000),
		errCh:  make(chan error, 10),
	}
}

func (c *CoinbaseProvider) Name() string {
	return c.name
}

func (c *CoinbaseProvider) Connect(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, coinbaseWSURL, nil)
	if err != nil {
		return fmt.Errorf("coinbase connect failed: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	log.Printf("[%s] Connected to WebSocket", c.name)
	return nil
}

func (c *CoinbaseProvider) Subscribe(symbols []string) (<-chan *domain.Tick, <-chan error) {
	// Convert symbols: BTCUSDT -> BTC-USD
	productIDs := make([]string, len(symbols))
	for i, symbol := range symbols {
		// Simple conversion for common pairs
		productIDs[i] = convertToCoinbaseSymbol(symbol)
	}

	subMsg := map[string]any{
		"type":        "subscribe",
		"product_ids": productIDs,
		"channels":    []string{"ticker"},
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if err := conn.WriteJSON(subMsg); err != nil {
		c.errCh <- fmt.Errorf("coinbase subscribe failed: %w", err)
		return c.tickCh, c.errCh
	}

	log.Printf("[%s] Subscribed to %v", c.name, productIDs)

	go c.readLoop()

	return c.tickCh, c.errCh
}

func (c *CoinbaseProvider) readLoop() {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case c.errCh <- fmt.Errorf("coinbase read error: %w", err):
				case <-c.ctx.Done():
					return
				}
				return
			}

			c.handleMessage(message)
		}
	}
}

func (c *CoinbaseProvider) handleMessage(data []byte) {
	var ticker coinbaseTicker
	if err := json.Unmarshal(data, &ticker); err != nil {
		return
	}

	if ticker.Type != "ticker" {
		return
	}

	price, err := strconv.ParseFloat(ticker.Price, 64)
	if err != nil {
		return
	}

	// Use a default volume since ticker doesn't provide per-trade volume
	volume := 1.0

	// Convert back to standard symbol format
	symbol := convertFromCoinbaseSymbol(ticker.ProductID)

	tick := AcquireTick()
	tick.Exchange = c.name
	tick.Symbol = symbol
	tick.Price = price
	tick.Volume = volume
	tick.Timestamp = time.Now()

	select {
	case c.tickCh <- tick:
	case <-c.ctx.Done():
		ReleaseTick(tick)
		return
	default:
		ReleaseTick(tick)
	}
}

func (c *CoinbaseProvider) Close() error {
	if c.cancel != nil {
		c.cancel()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		if err != nil {
			log.Printf("[%s] Error sending close: %v", c.name, err)
		}

		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("coinbase close failed: %w", err)
		}

		log.Printf("[%s] Connection closed", c.name)
	}

	return nil
}

func convertToCoinbaseSymbol(symbol string) string {
	// BTCUSDT -> BTC-USD
	if len(symbol) >= 6 {
		base := symbol[:3]
		quote := symbol[3 : len(symbol)-1] // Remove T from USDT
		return fmt.Sprintf("%s-%s", base, quote)
	}
	return symbol
}

func convertFromCoinbaseSymbol(productID string) string {
	// BTC-USD -> BTCUSDT
	// Simple conversion for common pairs
	if len(productID) >= 7 {
		parts := productID[:]
		return parts[:3] + parts[4:] + "T" // Add T for tether
	}
	return productID
}
