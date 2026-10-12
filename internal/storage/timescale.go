// Package storage contains the TimescaleDB wiring for the raw, append-only tick
// store owned by market-data-service.
package storage

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
)

// Timescale wraps a pgx pool pointed at the TimescaleDB market_data database.
type Timescale struct {
	pool *pgxpool.Pool
}

// NewTimescale connects to TIMESCALE_DSN. A blank DSN returns a nil store so the
// service remains runnable standalone (ticks are simply dropped).
func NewTimescale(ctx context.Context, dsn string) (*Timescale, error) {
	if dsn == "" {
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Timescale{pool: pool}, nil
}

// Insert persists a single tick into the hypertable. Duplicate trade_id (from
// at-least-once delivery) is ignored via ON CONFLICT DO NOTHING.
func (t *Timescale) Insert(ctx context.Context, tr domain.Trade) error {
	if t == nil {
		return nil
	}
	ts := time.Unix(0, tr.Timestamp).UTC()
	_, err := t.pool.Exec(ctx, `
		INSERT INTO ticks (trade_id, symbol, price, quantity, timestamp_ns, ts,
			maker_order_id, taker_order_id, maker_account_id, taker_account_id)
		VALUES ($1, $2, $3::numeric, $4::numeric, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (trade_id, ts) DO NOTHING`,
		tr.TradeID, tr.Symbol, tr.Price, tr.Quantity, tr.Timestamp, ts,
		tr.MakerOrderID, tr.TakerOrderID, tr.MakerAccountID, tr.TakerAccountID,
	)
	return err
}

// Close releases the underlying pool.
func (t *Timescale) Close() {
	if t != nil && t.pool != nil {
		t.pool.Close()
	}
}
