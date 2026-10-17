// Package handler exposes the market-data service over HTTP/JSON. The response
// shapes intentionally match the gateway's previous market-data endpoints so the
// gateway can proxy them without transforming the payload.
package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/nguyen-anh-hao/market-data-service/internal/client"
	"github.com/nguyen-anh-hao/market-data-service/internal/service"
	"github.com/nguyen-anh-hao/market-data-service/internal/stream"
)

// Handler holds the read model, the engine client and the WebSocket hub.
type Handler struct {
	svc    *service.MarketDataService
	engine *client.EngineClient
	hub    *stream.Hub
}

// New wires the HTTP routes.
func New(svc *service.MarketDataService, engine *client.EngineClient, hub *stream.Hub) http.Handler {
	h := &Handler{svc: svc, engine: engine, hub: hub}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ticker", h.ticker)
	mux.HandleFunc("GET /book", h.orderBook)
	mux.HandleFunc("GET /trades-tape", h.tradesTape)
	mux.HandleFunc("GET /ohlcv", h.ohlcv)
	mux.HandleFunc("GET /ws", h.hub.Handler)
	mux.HandleFunc("GET /health", h.health)
	return mux
}

// ticker forwards to the matching engine (source of truth for last/bid/ask).
func (h *Handler) ticker(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	t, err := h.engine.Ticker(r.Context(), symbol)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "matching engine unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ticker": t})
}

// orderBook forwards to the matching engine (L2 snapshot).
func (h *Handler) orderBook(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	depth, _ := strconv.ParseUint(r.URL.Query().Get("depth"), 10, 32)
	book, err := h.engine.OrderBook(r.Context(), symbol, uint32(depth))
	if err != nil {
		writeErr(w, http.StatusBadGateway, "matching engine unavailable")
		return
	}
	writeJSON(w, http.StatusOK, book)
}

// tradesTape returns the public anonymized recent trade tape.
func (h *Handler) tradesTape(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	writeJSON(w, http.StatusOK, map[string]interface{}{"trades": h.svc.Tape(r.Context(), symbol)})
}

// ohlcv returns pre-aggregated OHLCV candles.
func (h *Handler) ohlcv(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	interval := r.URL.Query().Get("interval")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, http.StatusOK, map[string]interface{}{"candles": h.svc.Candles(r.Context(), symbol, interval, limit)})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
