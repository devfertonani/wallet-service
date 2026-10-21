package sqsconsumer_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/devfertonani/wallet-service/internal/adapters/sqsconsumer"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
)

func body(t *testing.T, provider string) string {
	t.Helper()
	b, _ := json.Marshal(map[string]any{
		"messageId": "m-1", "type": sqsconsumer.MessageType, "occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId": provider, "externalTransactionId": "t-1", "idempotencyKey": provider + ":t-1",
			"playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
			"roundId": "r", "gameId": "g", "kind": "BET", "money": map[string]string{"amount": "1.00", "currency": "BRL"},
		},
	})
	return string(b)
}

// Without a service, Handle can only answer for what it decides before the
// use case: envelope validation and the sender binding.
func consumer(senders map[string]string) *sqsconsumer.Consumer {
	return sqsconsumer.New(nil, nil, nil, sqsconsumer.Options{ProviderSenders: senders}, metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSenderMustMatchProvider(t *testing.T) {
	c := consumer(map[string]string{"AIDA-PROVIDER-A": "provider-a"})
	outcome, reason, _ := c.Handle(context.Background(), body(t, "provider-a"), "AIDA-PROVIDER-B")
	if outcome != sqsconsumer.Permanent || reason != "PROVIDER_NOT_AUTHORIZED" {
		t.Fatalf("outcome=%s reason=%s", outcome, reason)
	}
	outcome, reason, _ = c.Handle(context.Background(), body(t, "provider-b"), "AIDA-PROVIDER-A")
	if outcome != sqsconsumer.Permanent || reason != "PROVIDER_NOT_AUTHORIZED" {
		t.Fatalf("provider-a's sender speaking for provider-b: outcome=%s reason=%s", outcome, reason)
	}
}

func TestInvalidEnvelopes(t *testing.T) {
	c := consumer(nil)
	cases := map[string]string{
		"not json":     "{",
		"wrong type":   `{"messageId":"m","type":"Other","data":{}}`,
		"no messageId": `{"type":"WagerTransactionRequested","data":{"money":{"amount":"1.00","currency":"BRL"}}}`,
		"no money":     `{"messageId":"m","type":"WagerTransactionRequested","data":{}}`,
		"number money": `{"messageId":"m","type":"WagerTransactionRequested","data":{"money":{"amount":1.00,"currency":"BRL"}}}`,
	}
	for name, b := range cases {
		if outcome, _, _ := c.Handle(context.Background(), b, ""); outcome != sqsconsumer.Permanent {
			t.Errorf("%s: outcome = %s", name, outcome)
		}
	}
}
