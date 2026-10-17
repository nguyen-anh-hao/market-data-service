// Package eventbus subscribes the market-data service to trade events published
// by order-service over NATS JetStream.
package eventbus

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
)

const (
	// StreamName is the JetStream stream holding trade events.
	StreamName = "TRADES"
	// TradeSubject is the subject the gateway publishes executed trades to.
	TradeSubject = "market.trades.executed"
	// DurableName is the durable consumer name so replays resume from the last
	// acknowledged message after a restart.
	DurableName = "market-data-service"
)

// Subscriber consumes trade events and forwards them to onTrade.
type Subscriber struct {
	addr    string
	onTrade func(domain.Trade)
}

// NewSubscriber builds a subscriber. onTrade is invoked for every trade event.
func NewSubscriber(addr string, onTrade func(domain.Trade)) *Subscriber {
	return &Subscriber{addr: addr, onTrade: onTrade}
}

// Start begins the connect + subscribe loop on a background goroutine.
func (s *Subscriber) Start() {
	if s.addr == "" {
		slog.Warn("eventbus: NATS_ADDR unset, market data subscription disabled")
		return
	}
	go func() {
		for {
			if err := s.run(); err != nil {
				slog.Error("eventbus: subscriber error", "err", err)
			}
			time.Sleep(2 * time.Second)
		}
	}()
}

func (s *Subscriber) run() error {
	nc, err := nats.Connect(s.addr,
		nats.Timeout(2*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     StreamName,
		Subjects: []string{TradeSubject},
	}); err != nil {
		return err
	}

	sub, err := js.QueueSubscribe(TradeSubject, "market-data",
		func(m *nats.Msg) {
			var tr domain.Trade
			if err := json.Unmarshal(m.Data, &tr); err != nil {
				slog.Warn("eventbus: bad trade event", "err", err)
				_ = m.Ack()
				return
			}
			s.onTrade(tr)
			_ = m.Ack()
		},
		nats.Durable(DurableName),
		nats.DeliverNew(),
		nats.ManualAck(),
	)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	// Block forever: nats auto-reconnects indefinitely (MaxReconnects(-1)) and
	// re-establishes the durable subscription on reconnect. The process lifetime
	// owns this goroutine.
	select {}
}
