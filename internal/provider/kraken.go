package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/razedwell/crypto-stream-engine/internal/domain"
)

const krakenWSURL = "wss://ws.kraken.com/"

type KrakenProvider struct {
	name   string
	conn   *websocket.Conn
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	tickCh chan *domain.Tick
	errCh  chan error
}

func NewKrakenProvider() *KrakenProvider {
	return &KrakenProvider{
		name:   "kraken",
		tickCh: make(chan *domain.Tick, 1000),
		errCh:  make(chan error, 10),
	}
}

func (k *KrakenProvider) Name() string {
	return k.name
}

func (k *KrakenProvider) Connect(ctx context.Context) error {
	k.ctx, k.cancel = context.WithCancel(ctx)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, krakenWSURL, nil)
	if err != nil {
		return fmt.Errorf("kraken connect failed: %w", err)
	}

	k.mu.Lock()
	k.conn = conn
	k.mu.Unlock()

	log.Printf("[%s] Connected to WebSocket", k.name)
	return nil
}

func (k *KrakenProvider) Subscribe(symbols []string) (<-chan *domain.Tick, <-chan error) {
	// Convert symbols: BTCUSDT -> XBT/USDT
	pairs := make([]string, len(symbols))
	for i, symbol := range symbols {
		pairs[i] = convertToKrakenSymbol(symbol)
	}

	subMsg := map[string]any{
		"event": "subscribe",
		"pair":  pairs,
		"subscription": map[string]string{
			"name": "trade",
		},
	}

	k.mu.RLock()
	conn := k.conn
	k.mu.RUnlock()

	if err := conn.WriteJSON(subMsg); err != nil {
		k.errCh <- fmt.Errorf("kraken subscribe failed: %w", err)
		return k.tickCh, k.errCh
	}

	log.Printf("[%s] Subscribed to %v", k.name, pairs)

	go k.readLoop()

	return k.tickCh, k.errCh
}

func (k *KrakenProvider) readLoop() {
	k.mu.RLock()
	conn := k.conn
	k.mu.RUnlock()

	for {
		select {
		case <-k.ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case k.errCh <- fmt.Errorf("kraken read error: %w", err):
				case <-k.ctx.Done():
					return
				}
				return
			}

			k.handleMessage(message)
		}
	}
}

func (k *KrakenProvider) handleMessage(data []byte) {
	// Kraken sends arrays, not objects
	var msg []interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		// Might be a status message (object), skip it
		return
	}

	// Trade messages are arrays with length >= 4
	if len(msg) < 4 {
		return
	}

	// Last element is the pair name
	pairName, ok := msg[len(msg)-1].(string)
	if !ok {
		return
	}

	// Second-to-last element is the channel name
	channelName, ok := msg[len(msg)-2].(string)
	if !ok || channelName != "trade" {
		return
	}

	// Trade data is in the second element (index 1)
	trades, ok := msg[1].([]interface{})
	if !ok {
		return
	}

	symbol := convertFromKrakenSymbol(pairName)

	// Process each trade in the array
	for _, trade := range trades {
		tradeArr, ok := trade.([]interface{})
		if !ok || len(tradeArr) < 3 {
			continue
		}

		// Trade format: [price, volume, time, side, orderType, misc]
		priceStr, ok := tradeArr[0].(string)
		if !ok {
			continue
		}

		volumeStr, ok := tradeArr[1].(string)
		if !ok {
			continue
		}

		var price, volume float64
		fmt.Sscanf(priceStr, "%f", &price)
		fmt.Sscanf(volumeStr, "%f", &volume)

		tick := AcquireTick()
		tick.Exchange = k.name
		tick.Symbol = symbol
		tick.Price = price
		tick.Volume = volume
		tick.Timestamp = time.Now()

		select {
		case k.tickCh <- tick:
		case <-k.ctx.Done():
			ReleaseTick(tick)
			return
		default:
			ReleaseTick(tick)
		}
	}
}

func (k *KrakenProvider) Close() error {
	if k.cancel != nil {
		k.cancel()
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	if k.conn != nil {
		err := k.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		if err != nil {
			log.Printf("[%s] Error sending close: %v", k.name, err)
		}

		if err := k.conn.Close(); err != nil {
			return fmt.Errorf("kraken close failed: %w", err)
		}

		log.Printf("[%s] Connection closed", k.name)
	}

	return nil
}

func convertToKrakenSymbol(symbol string) string {
	// BTCUSDT -> XBT/USDT (Kraken uses XBT for Bitcoin)
	if symbol[:3] == "BTC" {
		return "XBT/" + symbol[3:]
	}
	return symbol[:3] + "/" + symbol[3:]
}

func convertFromKrakenSymbol(pair string) string {
	// XBT/USDT -> BTCUSDT
	if pair[:3] == "XBT" {
		return "BTC" + pair[4:]
	}
	// Remove slash
	return pair[:3] + pair[4:]
}
