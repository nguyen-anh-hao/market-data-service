package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nguyen-anh-hao/market-data-service/internal/client"
	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/service"
)

func TestHubStreamsSnapshotAndTicker(t *testing.T) {
	engineSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol":"AAPL","last_price":"100.00","best_bid":"99.50","best_ask":"100.50"}`))
	}))
	defer engineSrv.Close()

	engine := client.NewEngineClient(strings.TrimPrefix(engineSrv.URL, "http://"))
	svc := service.New(nil, nil)
	hub := NewHub(engine, svc, time.Hour)

	wsSrv := httptest.NewServer(http.HandlerFunc(hub.Handler))
	defer wsSrv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "ws" + strings.TrimPrefix(wsSrv.URL, "http") + "/ws?symbols=AAPL&channels=ticker,candle,trade"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	var first Message
	if err := readJSON(ctx, conn, &first); err != nil {
		t.Fatal(err)
	}
	if first.Type != "snapshot" {
		t.Fatalf("first message type = %q", first.Type)
	}

	hub.broadcast("ticker", "AAPL", "", client.Ticker{
		Symbol: "AAPL", LastPrice: "101.00", BestBid: "100.50", BestAsk: "101.50",
	})

	var second Message
	if err := readJSON(ctx, conn, &second); err != nil {
		t.Fatal(err)
	}
	if second.Type != "ticker" || second.Symbol != "AAPL" {
		t.Fatalf("second message = %+v", second)
	}
}

func TestHubTickerPollEmitsLiveCandle(t *testing.T) {
	engineSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol":"AAPL","last_price":"100.00","best_bid":"99.50","best_ask":"100.50"}`))
	}))
	defer engineSrv.Close()

	engine := client.NewEngineClient(strings.TrimPrefix(engineSrv.URL, "http://"))
	svc := service.New(nil, nil)
	hub := NewHub(engine, svc, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsSrv := httptest.NewServer(http.HandlerFunc(hub.Handler))
	defer wsSrv.Close()

	url := "ws" + strings.TrimPrefix(wsSrv.URL, "http") + "/ws?symbols=AAPL&channels=ticker,candle"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	var snap Message
	if err := readJSON(ctx, conn, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Type != "snapshot" {
		t.Fatalf("first message type = %q", snap.Type)
	}

	// Start polling only after the snapshot so it cannot race the first frame.
	go hub.Run(ctx)

	type candleMessage struct {
		Type     string        `json:"type"`
		Symbol   string        `json:"symbol"`
		Interval string        `json:"interval"`
		Data     domain.Candle `json:"data"`
	}

	for i := 0; i < 40; i++ {
		typ, b, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if typ != websocket.MessageText {
			t.Fatalf("unexpected message type %v", typ)
		}
		var msg candleMessage
		if err := json.Unmarshal(b, &msg); err != nil {
			t.Fatal(err)
		}
		if msg.Type != "candle" {
			continue // snapshot ticker frames arrive first
		}
		if msg.Symbol != "AAPL" || msg.Interval != "1m" {
			t.Fatalf("unexpected candle envelope: %+v", msg)
		}
		if msg.Data.Open != 100 || msg.Data.High != 100 || msg.Data.Low != 100 || msg.Data.Close != 100 {
			t.Fatalf("unexpected ticker candle: %+v", msg.Data)
		}
		return
	}
	t.Fatal("hub did not emit a ticker-derived candle")
}

func TestSnapshotIncludesTickerDerivedCandle(t *testing.T) {
	engineSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol":"AAPL","last_price":"100.00","best_bid":"99.50","best_ask":"100.50"}`))
	}))
	defer engineSrv.Close()

	engine := client.NewEngineClient(strings.TrimPrefix(engineSrv.URL, "http://"))
	hub := NewHub(engine, service.New(nil, nil), time.Hour)
	hub.observeTicker("AAPL", client.Ticker{LastPrice: "123.45"})

	raw, err := hub.snapshotMessage(context.Background(), map[string]struct{}{"AAPL": {}})
	if err != nil {
		t.Fatal(err)
	}
	var msg struct {
		Type string `json:"type"`
		Data struct {
			Candles map[string][]domain.Candle `json:"candles"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	candles := msg.Data.Candles["AAPL"]
	if len(candles) != 1 || candles[0].Close != 123.45 {
		t.Fatalf("snapshot candles = %+v", candles)
	}
}

func TestRememberCandleMergesTickerHighLow(t *testing.T) {
	hub := NewHub(nil, service.New(nil, nil), time.Hour)
	hub.observeTicker("AAPL", client.Ticker{LastPrice: "100"})
	hub.observeTicker("AAPL", client.Ticker{LastPrice: "110"})

	live, ok := hub.liveCandle("AAPL")
	if !ok {
		t.Fatal("no live candle")
	}
	merged, ok := hub.rememberCandle("AAPL", domain.Candle{
		Ts: live.Ts, Open: 101, High: 105, Low: 95, Close: 105, Volume: 7,
	})
	if !ok {
		t.Fatal("authoritative candle was rejected")
	}
	if merged.High != 110 || merged.Low != 95 || merged.Volume != 7 {
		t.Fatalf("merged candle = %+v", merged)
	}
}

func TestTickerSymbolsIncludesCandleOnlySubscribers(t *testing.T) {
	hub := NewHub(nil, service.New(nil, nil), time.Hour)
	hub.clients[&wsClient{
		symbols:  map[string]struct{}{"AAPL": {}},
		channels: map[string]struct{}{"candle": {}},
	}] = struct{}{}

	got := hub.tickerSymbols()
	if len(got) != 1 || got[0] != "AAPL" {
		t.Fatalf("tickerSymbols() = %v, want [AAPL]", got)
	}
}

func readJSON(ctx context.Context, conn *websocket.Conn, out interface{}) error {
	typ, b, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if typ != websocket.MessageText {
		return fmt.Errorf("unexpected message type %v", typ)
	}
	return json.Unmarshal(b, out)
}
