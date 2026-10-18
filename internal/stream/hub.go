// Package stream exposes the public market-data WebSocket stream. One server
// process polls the matching engine once per subscribed symbol and fans the
// result out to every client, so browser polling (N clients x M symbols) becomes
// a single shared set of upstream reads.
package stream

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/nguyen-anh-hao/market-data-service/internal/client"
	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/metrics"
	"github.com/nguyen-anh-hao/market-data-service/internal/service"
)

var (
	wsConnections   = metrics.Default.Gauge("trading_ws_connections", "Connected WebSocket clients.")
	wsDropped       = metrics.Default.Counter("trading_ws_dropped_total", "WebSocket frames dropped under backpressure.")
	wsSent          = metrics.Default.Counter("trading_ws_messages_sent_total", "WebSocket messages written.")
	wsTickerCandles = metrics.Default.Counter("trading_ws_ticker_candles_total", "Live 1m candles derived from ticker polls.", "symbol")
)

const (
	maxSymbols   = 64
	clientBuffer = 128
	writeTimeout = 5 * time.Second
	pingInterval = 30 * time.Second
)

// Message is the WebSocket wire envelope.
type Message struct {
	Type     string      `json:"type"`
	Symbol   string      `json:"symbol,omitempty"`
	Interval string      `json:"interval,omitempty"`
	Data     interface{} `json:"data,omitempty"`
}

type snapshot struct {
	Tickers []client.Ticker            `json:"tickers"`
	Candles map[string][]domain.Candle `json:"candles"`
}

type wsClient struct {
	conn     *websocket.Conn
	send     chan []byte
	symbols  map[string]struct{}
	channels map[string]struct{}
}

// Hub tracks connected clients and broadcasts market-data updates.
type Hub struct {
	engine      *client.EngineClient
	svc         *service.MarketDataService
	tickerEvery time.Duration

	mu      sync.RWMutex
	clients map[*wsClient]struct{}

	// liveMu guards the ticker-derived 1m candle per symbol. This keeps the
	// chart moving even when NATS is disabled or no trade event has arrived yet.
	liveMu      sync.Mutex
	liveCandles map[string]domain.Candle
}

func NewHub(engine *client.EngineClient, svc *service.MarketDataService, tickerEvery time.Duration) *Hub {
	if tickerEvery <= 0 {
		tickerEvery = 500 * time.Millisecond
	}
	return &Hub{
		engine:      engine,
		svc:         svc,
		tickerEvery: tickerEvery,
		clients:     map[*wsClient]struct{}{},
		liveCandles: map[string]domain.Candle{},
	}
}

// Handler upgrades GET /ws and keeps the connection registered until it drops.
func (h *Hub) Handler(w http.ResponseWriter, r *http.Request) {
	symbols := parseSymbols(r.URL.Query().Get("symbols"))
	if len(symbols) == 0 {
		http.Error(w, `{"error":"symbols query parameter required"}`, http.StatusBadRequest)
		return
	}
	channels := parseChannels(r.URL.Query().Get("channels"))

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Public, read-only market data: any origin may subscribe. Deployments
		// that need origin pinning can add a check here.
		InsecureSkipVerify: true,
	})
	if err != nil {
		slog.Warn("websocket accept failed", "err", err)
		return
	}
	conn.SetReadLimit(1024)

	c := &wsClient{
		conn:     conn,
		send:     make(chan []byte, clientBuffer),
		symbols:  symbols,
		channels: channels,
	}
	h.add(c)
	defer h.remove(c)

	ctx := conn.CloseRead(r.Context())
	go c.writeLoop(ctx)

	if msg, err := h.snapshotMessage(ctx, symbols); err == nil {
		c.enqueue(msg)
	}
	<-ctx.Done()
	conn.CloseNow()
}

func (c *wsClient) writeLoop(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case msg := <-c.send:
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				c.conn.CloseNow()
				return
			}
			wsSent.Inc()
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Ping(pctx)
			cancel()
			if err != nil {
				c.conn.CloseNow()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *wsClient) enqueue(msg []byte) {
	select {
	case c.send <- msg:
	default:
		// Slow consumer: drop the frame instead of blocking the hub. The next
		// ticker/candle update will resynchronise the client.
		wsDropped.Inc()
	}
}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	n := len(h.clients)
	h.mu.Unlock()
	wsConnections.Set(float64(n))
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	n := len(h.clients)
	h.mu.Unlock()
	wsConnections.Set(float64(n))
}

func parseSymbols(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		s := strings.ToUpper(strings.TrimSpace(part))
		if s == "" {
			continue
		}
		out[s] = struct{}{}
		if len(out) >= maxSymbols {
			break
		}
	}
	return out
}

func parseChannels(raw string) map[string]struct{} {
	if strings.TrimSpace(raw) == "" {
		raw = "ticker,candle,trade"
	}
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		c := strings.ToLower(strings.TrimSpace(part))
		switch c {
		case "ticker", "candle", "trade":
			out[c] = struct{}{}
		}
	}
	return out
}

func encode(msg Message) []byte {
	b, _ := json.Marshal(msg)
	return b
}
