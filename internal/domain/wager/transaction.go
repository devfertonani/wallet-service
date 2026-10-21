// Package wager models the wager transaction, its state machine and the rules
// of the five external kinds.
package wager

import (
	"errors"
	"fmt"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/google/uuid"
)

type Kind string

const (
	Opening  Kind = "OPENING"
	Bet      Kind = "BET"
	Win      Kind = "WIN"
	Loss     Kind = "LOSS"
	Refund   Kind = "REFUND"
	Rollback Kind = "ROLLBACK"
)

func ParseKind(s string) (Kind, error) {
	switch Kind(s) {
	case Opening, Bet, Win, Loss, Refund, Rollback:
		return Kind(s), nil
	}
	return "", failure.New(failure.UnsupportedKind, "unknown kind %q", s)
}

// External reports whether providers may send the kind. OPENING is internal.
func (k Kind) External() bool { return k != Opening }

func (k Kind) RequiresReference() bool { return k == Refund || k == Rollback }

type Status string

const (
	Pending          Status = "PENDING"
	PendingReference Status = "PENDING_REFERENCE"
	Processed        Status = "PROCESSED"
	Rejected         Status = "REJECTED"
	Failed           Status = "FAILED"
)

func ParseStatus(s string) (Status, error) {
	switch Status(s) {
	case Pending, PendingReference, Processed, Rejected, Failed:
		return Status(s), nil
	}
	return "", fmt.Errorf("%w: status %q", ErrInvalidTransaction, s)
}

func (s Status) Terminal() bool { return s == Processed || s == Rejected || s == Failed }

type Origin string

const (
	Internal Origin = "INTERNAL"
	External Origin = "EXTERNAL"
)

var (
	ErrInvalidTransaction = errors.New("wager: invalid transaction")
	// ErrInvalidTransition is returned for any transition the state machine
	// does not allow, including every transition out of a terminal state.
	ErrInvalidTransition = errors.New("wager: invalid state transition")
)

// Transaction is a wager transaction. State changes only through the
// transition methods, which enforce the state machine:
//
//	PENDING           -> PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
//	PENDING_REFERENCE -> PROCESSED | REJECTED | FAILED (and rescheduling)
type Transaction struct {
	s Snapshot
}

// Snapshot is the plain persisted form of a transaction, used by repositories
// to write and to rehydrate. It is a copy: changing it does not change the
// transaction.
type Snapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Money                          money.Money
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID // uuid.Nil when unresolved
	FailureCode                    failure.Code
	ResultBalance                  *money.Money
	CorrelationID                  string
	Attempts                       int
	NextAttemptAt                  *time.Time
	ExpiresAt                      *time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

// NewExternal registers an external operation in PENDING.
func NewExternal(id uuid.UUID, c Command, correlationID string, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || c.payloadHash == "" {
		return nil, fmt.Errorf("%w: missing identity or unvalidated command", ErrInvalidTransaction)
	}
	now = now.UTC()
	return &Transaction{s: Snapshot{
		ID: id, Origin: External, Kind: c.kind, Status: Pending,
		WalletID: c.walletID, PlayerID: c.playerID, Money: c.money,
		ProviderID: c.providerID, ExternalTransactionID: c.externalID, IdempotencyKey: c.idempotencyKey,
		PayloadHash: c.payloadHash, RoundID: c.roundID, GameID: c.gameID,
		ReferenceExternalTransactionID: c.referenceExterID, CorrelationID: correlationID,
		CreatedAt: now, UpdatedAt: now,
	}}, nil
}

// NewOpening creates the internal OPENING of a wallet with a positive initial
// balance, already PROCESSED: it has no dependency and commits with the wallet.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing identity", ErrInvalidTransaction)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: OPENING requires a positive amount", ErrInvalidTransaction)
	}
	now = now.UTC()
	t := &Transaction{s: Snapshot{
		ID: id, Origin: Internal, Kind: Opening, Status: Pending,
		WalletID: walletID, PlayerID: playerID, Money: amount, CorrelationID: id.String(),
		CreatedAt: now, UpdatedAt: now,
	}}
	if err := t.Process(amount, uuid.Nil, now); err != nil {
		return nil, err
	}
	return t, nil
}

// Rehydrate rebuilds a stored transaction. It checks that the stored shape is
// coherent but replays no transition.
func Rehydrate(s Snapshot) (*Transaction, error) {
	bad := func(why string) (*Transaction, error) {
		return nil, fmt.Errorf("%w: stored transaction %s: %s", ErrInvalidTransaction, s.ID, why)
	}
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil || !s.Money.IsValid() {
		return bad("missing identity or money")
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return bad("kind")
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return bad("status")
	}
	if (s.Origin == Internal) != (s.Kind == Opening) {
		return bad("origin does not match kind")
	}
	if s.Origin == External && (s.ProviderID == "" || s.ExternalTransactionID == "" || s.PayloadHash == "") {
		return bad("missing external metadata")
	}
	if s.Status == Processed && s.ResultBalance == nil {
		return bad("processed without result balance")
	}
	if (s.Status == Rejected || s.Status == Failed) && s.FailureCode == "" {
		return bad("terminal failure without code")
	}
	if s.Status == PendingReference && (s.NextAttemptAt == nil || s.ExpiresAt == nil) {
		return bad("pending reference without schedule")
	}
	return &Transaction{s: s}, nil
}

func (t *Transaction) transition(to Status, now time.Time) error {
	from := t.s.Status
	ok := false
	switch from {
	case Pending:
		ok = to == Processed || to == Rejected || to == Failed || to == PendingReference
	case PendingReference:
		ok = to == Processed || to == Rejected || to == Failed
	}
	if !ok {
		return fmt.Errorf("%w: %s -> %s (transaction %s)", ErrInvalidTransition, from, to, t.s.ID)
	}
	t.s.Status = to
	t.s.UpdatedAt = now.UTC()
	return nil
}

// Process concludes the operation. resultBalance is the wallet balance after
// it, returned unchanged by every later replay. referenceID is the resolved
// internal reference, or uuid.Nil.
func (t *Transaction) Process(resultBalance money.Money, referenceID uuid.UUID, now time.Time) error {
	if !resultBalance.IsValid() || resultBalance.IsNegative() {
		return fmt.Errorf("%w: invalid result balance", ErrInvalidTransaction)
	}
	if err := t.transition(Processed, now); err != nil {
		return err
	}
	rb := resultBalance
	t.s.ResultBalance = &rb
	t.s.ReferenceTransactionID = referenceID
	t.clearSchedule()
	return nil
}

// Reject records a definitive business rejection. observedBalance is the
// balance seen when deciding, when there was one.
func (t *Transaction) Reject(code failure.Code, observedBalance *money.Money, now time.Time) error {
	if !code.Definitive() {
		return fmt.Errorf("%w: %s is not a business rejection code", ErrInvalidTransaction, code)
	}
	if err := t.transition(Rejected, now); err != nil {
		return err
	}
	t.s.FailureCode = code
	t.s.ResultBalance = observedBalance
	t.clearSchedule()
	return nil
}

// Fail records a permanent infrastructure failure for audit.
func (t *Transaction) Fail(code failure.Code, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: missing failure code", ErrInvalidTransaction)
	}
	if err := t.transition(Failed, now); err != nil {
		return err
	}
	t.s.FailureCode = code
	t.clearSchedule()
	return nil
}

// AwaitReference parks a PENDING operation until its reference arrives.
func (t *Transaction) AwaitReference(nextAttempt, expires time.Time, now time.Time) error {
	if !expires.After(now) || nextAttempt.Before(now) {
		return fmt.Errorf("%w: invalid pending schedule", ErrInvalidTransaction)
	}
	if err := t.transition(PendingReference, now); err != nil {
		return err
	}
	n, e := nextAttempt.UTC(), expires.UTC()
	t.s.NextAttemptAt, t.s.ExpiresAt = &n, &e
	return nil
}

// Reschedule records one more unsuccessful attempt of a PENDING_REFERENCE
// operation. It is bookkeeping, not a state change.
func (t *Transaction) Reschedule(nextAttempt, now time.Time) error {
	if t.s.Status != PendingReference {
		return fmt.Errorf("%w: reschedule from %s", ErrInvalidTransition, t.s.Status)
	}
	t.s.Attempts++
	n := nextAttempt.UTC()
	t.s.NextAttemptAt = &n
	t.s.UpdatedAt = now.UTC()
	return nil
}

func (t *Transaction) clearSchedule() {
	t.s.NextAttemptAt = nil
	t.s.ExpiresAt = nil
}

// Snapshot returns a copy of the state for persistence and responses.
func (t *Transaction) Snapshot() Snapshot {
	s := t.s
	if s.ResultBalance != nil {
		rb := *s.ResultBalance
		s.ResultBalance = &rb
	}
	return s
}

func (t *Transaction) ID() uuid.UUID             { return t.s.ID }
func (t *Transaction) Kind() Kind                { return t.s.Kind }
func (t *Transaction) Status() Status            { return t.s.Status }
func (t *Transaction) WalletID() uuid.UUID       { return t.s.WalletID }
func (t *Transaction) Money() money.Money        { return t.s.Money }
func (t *Transaction) PayloadHash() string       { return t.s.PayloadHash }
func (t *Transaction) ProviderID() string        { return t.s.ProviderID }
func (t *Transaction) FailureCode() failure.Code { return t.s.FailureCode }
