package domain

// Candle is a single OHLCV bar for a symbol and interval. Ts is the bucket
// start in Unix seconds (consistent with the gateway's historical /ohlcv shape).
type Candle struct {
	Ts     int64   `json:"timestamp"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}
