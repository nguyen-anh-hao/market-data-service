package stream

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/nguyen-anh-hao/market-data-service/internal/client"
	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/service"
)

// Run polls tickers for the union of subscribed symbols and broadcasts them.
// It is a single upstream poller regardless of how many browsers are connected.
func (h *Hub) Run(ctx context.Context) {
	ticker := time.NewTicker(h.tickerEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.pollTickers(ctx)
		}
	}
}

func (h *Hub) pollTickers(ctx context.Context) {
	symbols := h.tickerSymbols()
	if len(symbols) == 0 {
		return
	}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, symbol := range symbols {
		symbol := symbol
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			t, err := h.engine.Ticker(cctx, symbol)
			cancel()
			if err != nil {
				return
			}
			h.broadcast("ticker", symbol, "", t)
		}()
	}
	wg.Wait()
}

func (h *Hub) tickerSymbols() []string {
	seen := map[string]struct{}{}
	h.mu.RLock()
	for c := range h.clients {
		if _, ok := c.channels["ticker"]; !ok {
			continue
		}
		for s := range c.symbols {
			seen[s] = struct{}{}
		}
	}
	h.mu.RUnlock()
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out
}

// BroadcastTrade publishes an anonymized trade to trade/candle subscribers.
func (h *Hub) BroadcastTrade(tr domain.Trade) {
	h.BroadcastTradeView(service.TradeView{
		TradeID: tr.TradeID, Symbol: tr.Symbol, Price: tr.Price,
		Quantity: tr.Quantity, Timestamp: tr.Timestamp,
	})
}

// BroadcastTradeView publishes an already-anonymized trade view.
func (h *Hub) BroadcastTradeView(tv service.TradeView) {
	h.broadcast("trade", tv.Symbol, "", tv)
}

// BroadcastCandle publishes the in-progress candle for chart subscribers.
func (h *Hub) BroadcastCandle(symbol, interval string, c domain.Candle) {
	h.broadcast("candle", symbol, interval, c)
}

func (h *Hub) broadcast(msgType, symbol, interval string, data interface{}) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.clients) == 0 {
		return
	}
	frame := encode(Message{Type: msgType, Symbol: symbol, Interval: interval, Data: data})
	for c := range h.clients {
		if _, ok := c.symbols[symbol]; !ok {
			continue
		}
		if _, ok := c.channels[msgType]; !ok {
			continue
		}
		c.enqueue(frame)
	}
}

// snapshotMessage builds the initial state: current tickers plus 1m candles so
// a newly connected chart can render immediately.
func (h *Hub) snapshotMessage(ctx context.Context, symbols map[string]struct{}) ([]byte, error) {
	snap := snapshot{Tickers: make([]client.Ticker, 0, len(symbols)), Candles: map[string][]domain.Candle{}}
	for symbol := range symbols {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		t, err := h.engine.Ticker(cctx, symbol)
		cancel()
		if err == nil {
			snap.Tickers = append(snap.Tickers, t)
		}
		if candles := h.svc.Candles(ctx, symbol, "1m", 120); len(candles) > 0 {
			snap.Candles[symbol] = candles
		}
	}
	return encode(Message{Type: "snapshot", Data: snap}), nil
}

// LogStats emits a compact connection count line (useful in Loki).
func (h *Hub) LogStats() {
	h.mu.RLock()
	n := len(h.clients)
	h.mu.RUnlock()
	slog.Debug("websocket clients", "count", n)
}
