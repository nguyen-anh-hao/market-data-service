// Package service owns the market-data read model. All business state (tape,
// current candles, candle history) lives in Redis, so any replica can serve any
// request and a restart loses nothing. TimescaleDB remains the durable tick log.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/metrics"
	"github.com/nguyen-anh-hao/market-data-service/internal/storage"
)

var (
	ticksIngested = metrics.Default.Counter("trading_market_ticks_ingested_total", "Trades ingested into the read model.", "symbol")
	ticksDeduped  = metrics.Default.Counter("trading_market_dedupe_total", "Duplicate trade events skipped.")
)

const (
	maxTape     = 2000
	maxCandles  = 1000
	tapeKey     = "md:tape"
	eventsKey   = "md:events"
	ingestedTTL = 24 * time.Hour
)

// SupportedIntervals lists the pre-aggregated candle intervals.
var SupportedIntervals = []string{"1s", "1m", "5m", "1h", "1d"}

// candleScript atomically rolls one interval. KEYS[1] = current-candle hash,
// KEYS[2] = completed-candle list. ARGV = bucket, price, qty, history limit.
var candleScript = redis.NewScript(`
local bucket = tonumber(ARGV[1])
local price = tonumber(ARGV[2])
local qty = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local raw = redis.call('HGET', KEYS[1], 'candle')
local curBucket = redis.call('HGET', KEYS[1], 'bucket')
local c
if not raw then
  c = {timestamp=bucket, open=price, high=price, low=price, close=price, volume=qty}
  redis.call('HSET', KEYS[1], 'bucket', bucket, 'candle', cjson.encode(c))
  return cjson.encode(c)
end
c = cjson.decode(raw)
if tonumber(curBucket) ~= bucket then
  redis.call('RPUSH', KEYS[2], raw)
  redis.call('LTRIM', KEYS[2], -limit, -1)
  c = {timestamp=bucket, open=price, high=price, low=price, close=price, volume=qty}
else
  if price > c.high then c.high = price end
  if price < c.low then c.low = price end
  c.close = price
  c.volume = c.volume + qty
end
redis.call('HSET', KEYS[1], 'bucket', bucket, 'candle', cjson.encode(c))
return cjson.encode(c)
`)

// TradeView is the public, anonymized projection of a trade returned by Tape.
type TradeView struct {
	TradeID   uint64 `json:"trade_id"`
	Symbol    string `json:"symbol"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Timestamp int64  `json:"timestamp_ns"`
}

// Update is broadcast over Redis Pub/Sub so every replica's WebSocket clients
// receive the same trade/candle event regardless of which replica ingested it.
type Update struct {
	Symbol   string         `json:"symbol"`
	Interval string         `json:"interval,omitempty"`
	Trade    *TradeView     `json:"trade,omitempty"`
	Candle   *domain.Candle `json:"candle,omitempty"`
}

// MarketDataService is stateless: Redis is the authoritative read model.
type MarketDataService struct {
	ts  *storage.Timescale
	rdb *redis.Client
}

func New(ts *storage.Timescale, rdb *redis.Client) *MarketDataService {
	return &MarketDataService{ts: ts, rdb: rdb}
}

// Ingest consumes one executed trade: persist it, append to the tape, roll every
// candle interval in Redis. It returns false for invalid or duplicate trade ids.
func (s *MarketDataService) Ingest(ctx context.Context, tr domain.Trade) bool {
	price, pok := parseFloat(tr.Price)
	qty, qok := parseFloat(tr.Quantity)
	if !pok || !qok || tr.Symbol == "" {
		return false
	}
	ticksIngested.Inc(tr.Symbol)
	s.persistTick(ctx, tr)
	if s.rdb == nil {
		return false
	}

	cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// NATS is at-least-once; dedupe by trade id so a redelivery cannot double
	// candle volume or duplicate tape rows.
	if tr.TradeID != 0 {
		ok, err := s.rdb.SetNX(cctx, fmt.Sprintf("md:ingested:%d", tr.TradeID), 1, ingestedTTL).Result()
		if err != nil {
			slog.Warn("market-data: dedupe check failed", "trade_id", tr.TradeID, "err", err)
		} else if !ok {
			ticksDeduped.Inc()
			return false
		}
	}

	if b, err := json.Marshal(tr); err == nil {
		pipe := s.rdb.TxPipeline()
		pipe.RPush(cctx, tapeKey, b)
		pipe.LTrim(cctx, tapeKey, -maxTape, -1)
		if _, err := pipe.Exec(cctx); err != nil {
			slog.Warn("market-data: tape write failed", "err", err)
		}
	}

	sec := tr.Timestamp / 1e9
	for _, iv := range SupportedIntervals {
		bucket := sec - sec%intervalSeconds(iv)
		if err := candleScript.Run(cctx, s.rdb,
			[]string{candleKey(tr.Symbol, iv), candleDoneKey(tr.Symbol, iv)},
			bucket, price, qty, maxCandles).Err(); err != nil {
			slog.Warn("market-data: candle update failed", "symbol", tr.Symbol, "interval", iv, "err", err)
		}
	}
	return true
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

// PublishUpdate fans a trade/candle update out to all market-data replicas.
func (s *MarketDataService) PublishUpdate(ctx context.Context, u Update) {
	if s.rdb == nil {
		return
	}
	b, err := json.Marshal(u)
	if err != nil {
		return
	}
	if err := s.rdb.Publish(ctx, eventsKey, b).Err(); err != nil {
		slog.Warn("market-data: pubsub publish failed", "err", err)
	}
}

// SubscribeUpdates receives cross-replica WebSocket updates.
func (s *MarketDataService) SubscribeUpdates(ctx context.Context) <-chan Update {
	ch := make(chan Update, 256)
	if s.rdb == nil {
		close(ch)
		return ch
	}
	go func() {
		defer close(ch)
		pubsub := s.rdb.Subscribe(ctx, eventsKey)
		defer pubsub.Close()
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Second)
				continue
			}
			var u Update
			if json.Unmarshal([]byte(msg.Payload), &u) != nil {
				continue
			}
			select {
			case ch <- u:
			default:
			}
		}
	}()
	return ch
}

// Tape returns the recent trade tape (ascending), optionally filtered by symbol.
func (s *MarketDataService) Tape(ctx context.Context, symbol string) []TradeView {
	if s.rdb == nil {
		return []TradeView{}
	}
	items, err := s.rdb.LRange(ctx, tapeKey, 0, -1).Result()
	if err != nil {
		slog.Warn("market-data: tape read failed", "err", err)
		return []TradeView{}
	}
	out := make([]TradeView, 0, len(items))
	for _, item := range items {
		var tr domain.Trade
		if json.Unmarshal([]byte(item), &tr) != nil {
			continue
		}
		if symbol != "" && tr.Symbol != symbol {
			continue
		}
		out = append(out, TradeView{
			TradeID: tr.TradeID, Symbol: tr.Symbol, Price: tr.Price,
			Quantity: tr.Quantity, Timestamp: tr.Timestamp,
		})
	}
	return out
}

// Candles returns OHLCV candles for a symbol + interval, ascending, capped to
// limit (0 = no limit). Unknown intervals fall back to 1m.
func (s *MarketDataService) Candles(ctx context.Context, symbol, interval string, limit int) []domain.Candle {
	if s.rdb == nil {
		return []domain.Candle{}
	}
	interval = normalizeInterval(interval)
	out := make([]domain.Candle, 0)
	if items, err := s.rdb.LRange(ctx, candleDoneKey(symbol, interval), 0, -1).Result(); err == nil {
		for _, item := range items {
			var c domain.Candle
			if json.Unmarshal([]byte(item), &c) == nil {
				out = append(out, c)
			}
		}
	}
	if cur, ok := s.CurrentCandle(ctx, symbol, interval); ok {
		out = append(out, cur)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ts < out[j].Ts })
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// CurrentCandle returns the in-progress candle for a symbol/interval.
func (s *MarketDataService) CurrentCandle(ctx context.Context, symbol, interval string) (domain.Candle, bool) {
	if s.rdb == nil {
		return domain.Candle{}, false
	}
	interval = normalizeInterval(interval)
	raw, err := s.rdb.HGet(ctx, candleKey(symbol, interval), "candle").Bytes()
	if err != nil {
		return domain.Candle{}, false
	}
	var c domain.Candle
	if json.Unmarshal(raw, &c) != nil {
		return domain.Candle{}, false
	}
	return c, true
}

func candleKey(symbol, interval string) string {
	return "md:candle:" + symbol + ":" + interval
}

func candleDoneKey(symbol, interval string) string {
	return "md:candle:done:" + symbol + ":" + interval
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
