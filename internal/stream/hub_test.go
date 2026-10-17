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
