package service

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
)

func TestIngestBuildsSharedTapeAndCandles(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	svc := New(nil, rdb)
	ctx := context.Background()

	svc.Ingest(ctx, domain.Trade{TradeID: 1, Symbol: "AAPL", Price: "100", Quantity: "2", Timestamp: 120_000_000_000})
	svc.Ingest(ctx, domain.Trade{TradeID: 2, Symbol: "AAPL", Price: "110", Quantity: "3", Timestamp: 125_000_000_000})

	cur, ok := svc.CurrentCandle(ctx, "AAPL", "1m")
	if !ok || cur.Ts != 120 || cur.Open != 100 || cur.High != 110 || cur.Close != 110 || cur.Volume != 5 {
		t.Fatalf("current candle = %+v, ok=%v", cur, ok)
	}

	svc.Ingest(ctx, domain.Trade{TradeID: 3, Symbol: "AAPL", Price: "105", Quantity: "1", Timestamp: 180_000_000_000})
	candles := svc.Candles(ctx, "AAPL", "1m", 10)
	if len(candles) != 2 {
		t.Fatalf("candles = %+v", candles)
	}
	if candles[0].Ts != 120 || candles[0].Close != 110 || candles[1].Ts != 180 || candles[1].Close != 105 {
		t.Fatalf("candles = %+v", candles)
	}

	tape := svc.Tape(ctx, "AAPL")
	if len(tape) != 3 || tape[0].TradeID != 1 || tape[2].TradeID != 3 {
		t.Fatalf("tape = %+v", tape)
	}
	if other := svc.Tape(ctx, "MSFT"); len(other) != 0 {
		t.Fatalf("symbol filter failed: %+v", other)
	}
}
