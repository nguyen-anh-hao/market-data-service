// Package domain defines the market-data read model: the raw trade wire contract
// and the aggregated OHLCV candle.
package domain

// Trade is the wire contract for an executed trade received from the NATS
// message bus. Field names mirror order-service/internal/eventbus.TradeEvent.
type Trade struct {
	TradeID        uint64 `json:"trade_id"`
	Symbol         string `json:"symbol"`
	Price          string `json:"price"`
	Quantity       string `json:"quantity"`
	Fee            string `json:"fee"`
	Role           string `json:"role"`
	Timestamp      int64  `json:"timestamp_ns"`
	MakerOrderID   uint64 `json:"maker_order_id"`
	TakerOrderID   uint64 `json:"taker_order_id"`
	MakerAccountID uint64 `json:"maker_account_id"`
	TakerAccountID uint64 `json:"taker_account_id"`
}
