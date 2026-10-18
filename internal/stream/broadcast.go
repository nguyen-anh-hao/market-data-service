package stream

import (
	"context"
	"log/slog"
	"math"
	"strconv"
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
			h.observeTicker(symbol, t)
		}()
	}
	wg.Wait()
}

func (h *Hub) tickerSymbols() []string {
	seen := map[string]struct{}{}
	h.mu.RLock()
	for c := range h.clients {
		// Candle subscribers need the poll too: it drives the live
		// ticker-derived candle fallback, not just the ticker fan-out.
		if _, ok := c.channels["ticker"]; !ok {
			if _, ok := c.channels["candle"]; !ok {
				continue
			}
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
// For 1m candles it is merged with the ticker-derived state first, so a poller
// candle and a NATS trade candle cannot overwrite each other's high/low.
func (h *Hub) BroadcastCandle(symbol, interval string, c domain.Candle) {
	if interval == "1m" {
		merged, ok := h.rememberCandle(symbol, c)
		if !ok {
			return // stale: the live state is already on a newer bucket
		}
		c = merged
	}
	h.broadcast("candle", symbol, interval, c)
}

// observeTicker folds a polled last price into the live 1m candle. The matching
// engine's last price only changes when a trade happens, so this is still a
// trade-priced candle; it covers the gap before/without a NATS candle event.
func (h *Hub) observeTicker(symbol string, t client.Ticker) {
	price, err := strconv.ParseFloat(t.LastPrice, 64)
	if err != nil || price <= 0 {
		return
	}

	now := time.Now().Unix()
	bucket := now - now%60

	h.liveMu.Lock()
	prev, ok := h.liveCandles[symbol]
	// At a minute boundary with an unchanged price no trade has happened in the
	// new bucket yet; keep the previous bar instead of fabricating a flat one.
	// This also lets the authoritative NATS candle for the old bucket arrive.
	if ok && bucket > prev.Ts && price == prev.Close {
		h.liveMu.Unlock()
		return
	}
	var cur domain.Candle
	if !ok || bucket > prev.Ts {
		cur = domain.Candle{Ts: bucket, Open: price, High: price, Low: price, Close: price}
	} else {
		// Same bucket (or a server clock slightly ahead of the browser).
		cur = prev
		cur.High = math.Max(cur.High, price)
		cur.Low = math.Min(cur.Low, price)
		cur.Close = price
	}
	h.liveCandles[symbol] = cur
	changed := !ok || cur.Ts != prev.Ts || cur.Close != prev.Close
	h.liveMu.Unlock()

	// Polls repeat the same price twice per second; only fan out real changes.
	if changed {
		wsTickerCandles.Inc(symbol)
		h.broadcast("candle", symbol, "1m", cur)
	}
}

// rememberCandle merges an authoritative NATS/Redis candle into the live state
// and returns the state to broadcast. A candle older than the live bucket is
// rejected so clients never receive an out-of-order bar for the chart.
func (h *Hub) rememberCandle(symbol string, c domain.Candle) (domain.Candle, bool) {
	h.liveMu.Lock()
	defer h.liveMu.Unlock()

	prev, ok := h.liveCandles[symbol]
	if ok && c.Ts < prev.Ts {
		return domain.Candle{}, false
	}
	if ok && c.Ts == prev.Ts {
		c.High = math.Max(c.High, prev.High)
		c.Low = math.Min(c.Low, prev.Low)
		if c.Open == 0 {
			c.Open = prev.Open
		}
	}
	h.liveCandles[symbol] = c
	return c, true
}

// liveCandle returns the ticker-derived current bar, if any.
func (h *Hub) liveCandle(symbol string) (domain.Candle, bool) {
	h.liveMu.Lock()
	c, ok := h.liveCandles[symbol]
	h.liveMu.Unlock()
	return c, ok
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
		if live, ok := h.liveCandle(symbol); ok {
			candles := snap.Candles[symbol]
			switch {
			case len(candles) == 0 || live.Ts > candles[len(candles)-1].Ts:
				candles = append(candles, live)
			case live.Ts == candles[len(candles)-1].Ts:
				// Keep the authoritative open/volume and fold in the latest
				// ticker-priced close/high/low.
				last := candles[len(candles)-1]
				last.High = math.Max(last.High, live.High)
				last.Low = math.Min(last.Low, live.Low)
				if live.Close > 0 {
					last.Close = live.Close
				}
				if last.Open == 0 {
					last.Open = live.Open
				}
				candles[len(candles)-1] = last
			}
			if len(candles) > 120 {
				candles = candles[len(candles)-120:]
			}
			if len(candles) > 0 {
				snap.Candles[symbol] = candles
			}
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
