// Package events defines the integration events written to the outbox. Each
// event has its own Go type; its constructor fixes eventType and version and
// serializes the envelope once, so the stored payload is an immutable snapshot.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/google/uuid"
)

const (
	TypeProcessed        = "WagerTransactionProcessed"
	TypeRejected         = "WagerTransactionRejected"
	TypeBalanceChanged   = "WalletBalanceChanged"
	TypePendingReference = "WagerTransactionPendingReference"

	// AggregateWallet is the aggregate type of every event: they are all
	// ordered per wallet (SNS MessageGroupId = aggregateId = walletId).
	AggregateWallet = "wallet"
)

// TimeFormat is RFC 3339 in UTC with milliseconds.
const TimeFormat = "2006-01-02T15:04:05.000Z07:00"

// Envelope is the wire format of every event.
type Envelope[T any] struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    string    `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          T         `json:"data"`
}

// Meta carries the tracing identifiers of the operation that caused the event.
type Meta struct {
	CorrelationID string
	CausationID   string
}

// Event is a serialized event ready for the outbox.
type Event struct {
	ID          uuid.UUID
	Type        string
	Version     int
	AggregateID uuid.UUID
	OccurredAt  time.Time
	Payload     []byte
}

type TransactionProcessed struct {
	TransactionID          uuid.UUID  `json:"transactionId"`
	WalletID               uuid.UUID  `json:"walletId"`
	PlayerID               uuid.UUID  `json:"playerId"`
	ProviderID             string     `json:"providerId,omitempty"`
	ExternalTransactionID  string     `json:"externalTransactionId,omitempty"`
	RoundID                string     `json:"roundId,omitempty"`
	GameID                 string     `json:"gameId,omitempty"`
	Kind                   string     `json:"kind"`
	Money                  money.DTO  `json:"money"`
	Balance                money.DTO  `json:"balance"`
	ReferenceTransactionID *uuid.UUID `json:"referenceTransactionId,omitempty"`
}

type TransactionRejected struct {
	TransactionID         uuid.UUID `json:"transactionId"`
	WalletID              uuid.UUID `json:"walletId"`
	PlayerID              uuid.UUID `json:"playerId"`
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	Kind                  string    `json:"kind"`
	Money                 money.DTO `json:"money"`
	FailureCode           string    `json:"failureCode"`
}

type BalanceChanged struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         money.DTO `json:"money"`
	BalanceBefore money.DTO `json:"balanceBefore"`
	BalanceAfter  money.DTO `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

type TransactionPendingReference struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	WalletID                       uuid.UUID `json:"walletId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	Kind                           string    `json:"kind"`
	Money                          money.DTO `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	ExpiresAt                      string    `json:"expiresAt"`
}

func build[T any](id uuid.UUID, eventType string, aggregate uuid.UUID, meta Meta, now time.Time, data T) (Event, error) {
	now = now.UTC()
	env := Envelope[T]{
		EventID: id, EventType: eventType, AggregateID: aggregate,
		CorrelationID: meta.CorrelationID, CausationID: meta.CausationID,
		OccurredAt: now.Format(TimeFormat), Version: 1, Data: data,
	}
	b, err := json.Marshal(env)
	if err != nil {
		return Event{}, fmt.Errorf("events: marshal %s: %w", eventType, err)
	}
	return Event{ID: id, Type: eventType, Version: 1, AggregateID: aggregate, OccurredAt: now, Payload: b}, nil
}

// NewProcessed is emitted for every successful operation, LOSS and OPENING included.
func NewProcessed(id uuid.UUID, t wager.Snapshot, meta Meta, now time.Time) (Event, error) {
	if t.Status != wager.Processed || t.ResultBalance == nil {
		return Event{}, fmt.Errorf("events: %s for transaction in %s", TypeProcessed, t.Status)
	}
	d := TransactionProcessed{
		TransactionID: t.ID, WalletID: t.WalletID, PlayerID: t.PlayerID, ProviderID: t.ProviderID,
		ExternalTransactionID: t.ExternalTransactionID, RoundID: t.RoundID, GameID: t.GameID,
		Kind: string(t.Kind), Money: t.Money.DTO(), Balance: t.ResultBalance.DTO(),
	}
	if t.ReferenceTransactionID != uuid.Nil {
		r := t.ReferenceTransactionID
		d.ReferenceTransactionID = &r
	}
	return build(id, TypeProcessed, t.WalletID, meta, now, d)
}

// NewRejected is emitted for a definitive business rejection.
func NewRejected(id uuid.UUID, t wager.Snapshot, meta Meta, now time.Time) (Event, error) {
	if t.Status != wager.Rejected {
		return Event{}, fmt.Errorf("events: %s for transaction in %s", TypeRejected, t.Status)
	}
	return build(id, TypeRejected, t.WalletID, meta, now, TransactionRejected{
		TransactionID: t.ID, WalletID: t.WalletID, PlayerID: t.PlayerID, ProviderID: t.ProviderID,
		ExternalTransactionID: t.ExternalTransactionID, Kind: string(t.Kind), Money: t.Money.DTO(),
		FailureCode: string(t.FailureCode),
	})
}

// NewBalanceChanged is emitted for every ledger entry.
func NewBalanceChanged(id uuid.UUID, e ledger.Entry, meta Meta, now time.Time) (Event, error) {
	return build(id, TypeBalanceChanged, e.WalletID(), meta, now, BalanceChanged{
		WalletID: e.WalletID(), TransactionID: e.TransactionID(), Direction: string(e.Direction()),
		Money: e.Amount().DTO(), BalanceBefore: e.BalanceBefore().DTO(), BalanceAfter: e.BalanceAfter().DTO(),
		WalletVersion: e.WalletVersion(),
	})
}

// NewPendingReference is emitted when an operation starts waiting for its reference.
func NewPendingReference(id uuid.UUID, t wager.Snapshot, meta Meta, now time.Time) (Event, error) {
	if t.Status != wager.PendingReference || t.ExpiresAt == nil {
		return Event{}, fmt.Errorf("events: %s for transaction in %s", TypePendingReference, t.Status)
	}
	return build(id, TypePendingReference, t.WalletID, meta, now, TransactionPendingReference{
		TransactionID: t.ID, WalletID: t.WalletID, ProviderID: t.ProviderID,
		ExternalTransactionID: t.ExternalTransactionID, Kind: string(t.Kind), Money: t.Money.DTO(),
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID,
		ExpiresAt:                      t.ExpiresAt.UTC().Format(TimeFormat),
	})
}
