// Package client contains the HTTP client used by market-data-service to forward
// ticker and order-book reads to the matching engine (the source of truth for
// those two views).
package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// EngineClient talks to the C++ matching engine over HTTP/JSON.
type EngineClient struct {
	baseURL string
	http    *http.Client
}

func NewEngineClient(addr string) *EngineClient {
	return &EngineClient{
		baseURL: "http://" + addr,
		http: &http.Client{
			Timeout:   5 * time.Second,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

// Ticker mirrors the engine's /ticker shape.
type Ticker struct {
	Symbol    string `json:"symbol"`
	LastPrice string `json:"last_price"`
	BestBid   string `json:"best_bid"`
	BestAsk   string `json:"best_ask"`
}

func (c *EngineClient) Ticker(ctx context.Context, symbol string) (Ticker, error) {
	var out Ticker
	u := c.baseURL + "/ticker?symbol=" + url.QueryEscape(symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return out, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}

// BookSnapshot mirrors the engine's /book shape.
type BookSnapshot struct {
	Symbol string     `json:"symbol"`
	Bids   [][]string `json:"bids"`
	Asks   [][]string `json:"asks"`
}

func (c *EngineClient) OrderBook(ctx context.Context, symbol string, depth uint32) (BookSnapshot, error) {
	var out BookSnapshot
	u := c.baseURL + "/book?symbol=" + url.QueryEscape(symbol) + "&depth=" + strconv.FormatUint(uint64(depth), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return out, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}
