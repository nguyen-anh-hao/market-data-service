// Package storage contains the Redis wiring for the authoritative market-data
// read model. Every replica reads/writes the same tape and candle keys.
package storage

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedis connects to REDIS_ADDR. A blank addr returns nil; main treats that
// as a fatal configuration error because the read model is Redis-backed.
func NewRedis(addr string) *redis.Client {
	if addr == "" {
		return nil
	}
	return redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})
}

// Available reports whether rdb is non-nil and reachable within a short budget.
func Available(ctx context.Context, rdb *redis.Client) bool {
	if rdb == nil {
		return false
	}
	cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	return rdb.Ping(cctx).Err() == nil
}
