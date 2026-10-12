// Package service owns the market-data read model: it consumes raw trade events,
// persists them to TimescaleDB (durable source of truth), and pre-aggregates
// OHLCV candles + the recent trade tape in memory (mirrored to Redis).
package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/storage"
)

const (
	maxTape    = 2000
	maxCandles = 1000
	tapeKey    = "md:tape" // Redis list (LPUSH most-recent-first)
)

// SupportedIntervals lists the pre-aggregated candle intervals.
var SupportedIntervals = []string{"1s", "1m", "5m", "1h", "1d"}

type symbolState struct {
	intervals map[string]*intervalState
}

type intervalState struct {
	currentBucket int64
	current       *domain.Candle
	done          []domain.Candle // ascending by timestamp
}

// TradeView is the public, anonymized projection of a trade returned by Tape.
type TradeView struct {
	TradeID   uint64 `json:"trade_id"`
	Symbol    string `json:"symbol"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Timestamp int64  `json:"timestamp_ns"`
}

// MarketDataService is the market-data read model.
type MarketDataService struct {
	ts  *storage.Timescale
	rdb *redis.Client

	mu     sync.RWMutex
	tape   []domain.Trade
	states map[string]*symbolState
}

// New wires the read model with its durable (TimescaleDB) and cache (Redis)
// stores. Either may be nil to keep the service runnable standalone.
func New(ts *storage.Timescale, rdb *redis.Client) *MarketDataService {
	return &MarketDataService{
		ts:     ts,
		rdb:    rdb,
		states: map[string]*symbolState{},
	}
}

// Ingest consumes one executed trade: append to the tape, roll the candles and
// (best-effort) persist to TimescaleDB + mirror to Redis.
func (s *MarketDataService) Ingest(ctx context.Context, tr domain.Trade) {
	price, pok := parseFloat(tr.Price)
	qty, qok := parseFloat(tr.Quantity)
	if !pok || !qok || tr.Symbol == "" {
		return
	}

	s.persistTick(ctx, tr)
	s.rollCandles(tr, price, qty)

	s.mu.Lock()
	s.tape = append(s.tape, tr)
	if len(s.tape) > maxTape {
		s.tape = s.tape[len(s.tape)-maxTape:]
	}
	s.mu.Unlock()

	s.mirrorTape(ctx, tr)
}

func (s *MarketDataService) persistTick(ctx context.Context, tr domain.Trade) {
	if s.ts == nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.ts.Insert(cctx, tr); err != nil {
		slog.Warn("market-data: tick insert failed", "trade_id", tr.TradeID, "err", err)
	}
}

func (s *MarketDataService) rollCandles(tr domain.Trade, price, qty float64) {
	sec := tr.Timestamp / 1e9
	s.mu.Lock()
	st := s.states[tr.Symbol]
	if st == nil {
		st = &symbolState{intervals: map[string]*intervalState{}}
		s.states[tr.Symbol] = st
	}
	for _, iv := range SupportedIntervals {
		bucket := sec - sec%intervalSeconds(iv)
		is := st.intervals[iv]
		if is == nil {
			is = &intervalState{}
			st.intervals[iv] = is
		}
		if is.current == nil || is.currentBucket != bucket {
			if is.current != nil {
				is.done = append(is.done, *is.current)
				if len(is.done) > maxCandles {
					is.done = is.done[len(is.done)-maxCandles:]
				}
			}
			is.current = &domain.Candle{Ts: bucket, Open: price, High: price, Low: price, Close: price, Volume: qty}
			is.currentBucket = bucket
		} else {
			c := is.current
			if price > c.High {
				c.High = price
			}
			if price < c.Low {
				c.Low = price
			}
			c.Close = price
			c.Volume += qty
		}
	}
	s.mu.Unlock()
}

func (s *MarketDataService) mirrorTape(ctx context.Context, tr domain.Trade) {
	if s.rdb == nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_ = s.rdb.LPush(cctx, tapeKey, mustJSON(tr)).Err()
	_ = s.rdb.LTrim(cctx, tapeKey, 0, maxTape-1).Err()
}

// Tape returns the recent trade tape (ascending), optionally filtered by symbol.
func (s *MarketDataService) Tape(symbol string) []TradeView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TradeView, 0, len(s.tape))
	for _, t := range s.tape {
		if symbol != "" && t.Symbol != symbol {
			continue
		}
		out = append(out, TradeView{
			TradeID:   t.TradeID,
			Symbol:    t.Symbol,
			Price:     t.Price,
			Quantity:  t.Quantity,
			Timestamp: t.Timestamp,
		})
	}
	return out
}

// Candles returns OHLCV candles for a symbol + interval, ascending, capped to
// limit (0 = no limit). Unknown intervals fall back to 1m.
func (s *MarketDataService) Candles(symbol, interval string, limit int) []domain.Candle {
	interval = normalizeInterval(interval)
	s.mu.RLock()
	var done []domain.Candle
	var current *domain.Candle
	if st := s.states[symbol]; st != nil {
		if is := st.intervals[interval]; is != nil {
			done = append([]domain.Candle{}, is.done...)
			if is.current != nil {
				c := *is.current
				current = &c
			}
		}
	}
	s.mu.RUnlock()

	out := done
	if current != nil {
		out = append(out, *current)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ts < out[j].Ts })
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func normalizeInterval(iv string) string {
	for _, s := range SupportedIntervals {
		if s == iv {
			return s
		}
	}
	return "1m"
}

func intervalSeconds(interval string) int64 {
	switch interval {
	case "1s":
		return 1
	case "5m":
		return 5 * 60
	case "1h":
		return 60 * 60
	case "1d":
		return 24 * 60 * 60
	default:
		return 60
	}
}

func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
