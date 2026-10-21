package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/events"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
)

func TestOpeningEventsAndEnvelope(t *testing.T) {
	amount, _ := money.Parse("1000.00", "BRL")
	walletID, playerID, txID := uuid.New(), uuid.New(), uuid.New()
	now := time.Date(2026, 9, 8, 12, 0, 0, 123_000_000, time.FixedZone("BRT", -3*3600))
	o, err := wallet.Open(walletID, playerID, amount, txID, uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	opening, err := wager.NewOpening(txID, walletID, playerID, amount, now)
	if err != nil {
		t.Fatal(err)
	}
	meta := events.Meta{CorrelationID: txID.String()}

	processed, err := events.NewProcessed(uuid.New(), opening.Snapshot(), meta, now)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := events.NewBalanceChanged(uuid.New(), *o.Entry, meta, now)
	if err != nil {
		t.Fatal(err)
	}

	var env map[string]any
	if err := json.Unmarshal(changed.Payload, &env); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "occurredAt", "version", "data"} {
		if _, ok := env[k]; !ok {
			t.Errorf("envelope lacks %s", k)
		}
	}
	if _, ok := env["causationId"]; ok {
		t.Error("empty causationId must be omitted")
	}
	if env["eventType"] != events.TypeBalanceChanged || env["version"].(float64) != 1 || env["aggregateId"] != walletID.String() {
		t.Fatalf("envelope = %v", env)
	}
	if env["occurredAt"] != "2026-09-08T15:00:00.123Z" {
		t.Fatalf("occurredAt = %v, want UTC RFC 3339 with milliseconds", env["occurredAt"])
	}
	data := env["data"].(map[string]any)
	for _, k := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[k]; !ok {
			t.Errorf("WalletBalanceChanged lacks %s", k)
		}
	}
	if data["balanceAfter"].(map[string]any)["amount"] != "1000.00" || data["direction"] != "CREDIT" {
		t.Fatalf("data = %v", data)
	}

	var p map[string]any
	_ = json.Unmarshal(processed.Payload, &p)
	pd := p["data"].(map[string]any)
	if pd["kind"] != "OPENING" || pd["providerId"] != nil || pd["externalTransactionId"] != nil {
		t.Fatalf("OPENING carries external metadata: %v", pd)
	}
}

func TestConstructorsCheckTheState(t *testing.T) {
	amount, _ := money.Parse("1.00", "BRL")
	c, err := wager.NewCommand(wager.CommandInput{
		ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k", PlayerID: uuid.NewString(), WalletID: uuid.NewString(),
		RoundID: "r", GameID: "g", Kind: "BET", Amount: "1.00", Currency: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := wager.NewExternal(uuid.New(), c, "corr", time.Now())
	if _, err := events.NewProcessed(uuid.New(), tx.Snapshot(), events.Meta{}, time.Now()); err == nil {
		t.Error("Processed event for a PENDING transaction")
	}
	if _, err := events.NewRejected(uuid.New(), tx.Snapshot(), events.Meta{}, time.Now()); err == nil {
		t.Error("Rejected event for a PENDING transaction")
	}
	if _, err := events.NewPendingReference(uuid.New(), tx.Snapshot(), events.Meta{}, time.Now()); err == nil {
		t.Error("PendingReference event for a PENDING transaction")
	}
	_ = tx.Process(amount, uuid.Nil, time.Now())
	if _, err := events.NewProcessed(uuid.New(), tx.Snapshot(), events.Meta{}, time.Now()); err != nil {
		t.Errorf("Processed event: %v", err)
	}
}
