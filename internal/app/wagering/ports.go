// Package wagering holds the use cases of the wallet: opening wallets,
// processing external operations from HTTP and SQS, resuming operations that
// wait for a reference, reads and reconciliation. It depends on the domain and
// on the ports below, never on pgx, HTTP or SQS.
package wagering

import (
	"context"
	"errors"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/events"
	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
)

// Errors the adapters translate from the database. Use errors.Is.
var (
	ErrNotFound = errors.New("not found")
	// ErrWalletExists: a wallet already exists for the player and currency.
	ErrWalletExists = errors.New("wallet already exists for player and currency")
	// ErrDuplicate: a concurrent writer inserted the same operation first
	// (unique index on provider + idempotency key or external id).
	ErrDuplicate = errors.New("duplicate operation")
	// ErrConcurrentUpdate: the conditional wallet update matched no row.
	ErrConcurrentUpdate = errors.New("wallet version changed concurrently")
	// ErrTransient wraps every failure that may succeed on retry with the same
	// input: connection loss, timeouts, lock timeout, serialization failure,
	// deadlock. Nothing was committed.
	ErrTransient = errors.New("transient failure")
	// ErrIntegrity: the database refused a state the domain accepted.
	ErrIntegrity = errors.New("integrity violation")
	// ErrLockConflict accompanies ErrTransient when the cause was waiting for
	// a lock: lock_timeout or a deadlock.
	ErrLockConflict = errors.New("lock conflict")
)

// Errors of the use cases, mapped to the contract by the adapters.
var (
	ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different payload")
	ErrDuplicateExternal    = errors.New("external transaction already processed with another idempotency key")
	ErrMessageIDReused      = errors.New("message id reused with a different payload")
)

type WalletRepository interface {
	Insert(ctx context.Context, w wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (wallet.Wallet, error)
	// GetForUpdate reads the wallet holding its row lock until the end of the
	// unit of work.
	GetForUpdate(ctx context.Context, id uuid.UUID) (wallet.Wallet, error)
	// Update writes the new balance and version only if the stored version is
	// still prevVersion.
	Update(ctx context.Context, w wallet.Wallet, prevVersion int64) error
}

type TransactionRepository interface {
	Insert(ctx context.Context, t *wager.Transaction) error
	Update(ctx context.Context, t *wager.Transaction) error
	Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetByIdempotencyKey(ctx context.Context, providerID, key string) (*wager.Transaction, error)
	GetByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// IsReversed reports whether a REFUND or ROLLBACK of the transaction was processed.
	IsReversed(ctx context.Context, id uuid.UUID) (bool, error)
	// WakeWaiting brings forward the next attempt of the operations waiting
	// for (providerID, externalID). Rows a worker holds are skipped.
	WakeWaiting(ctx context.Context, providerID, externalID string, now time.Time) error
	// DuePending lists PENDING_REFERENCE operations whose next attempt is due,
	// without locking them.
	DuePending(ctx context.Context, now time.Time, limit int) ([]PendingRef, error)
	// LockDuePending locks one due PENDING_REFERENCE operation, skipping it
	// (nil, nil) if another worker holds it or it is no longer due.
	LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error)
}

type PendingRef struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

type LedgerRepository interface {
	Append(ctx context.Context, e ledger.Entry) error
	// Page returns up to limit entries after the cursor (a ledger sequence
	// number; 0 for the first page), in ascending order.
	Page(ctx context.Context, walletID uuid.UUID, after int64, limit int) ([]LedgerRow, error)
	// Totals returns credits minus debits and the number of entries.
	Totals(ctx context.Context, walletID uuid.UUID, currency money.Currency) (money.Money, int64, error)
}

type LedgerRow struct {
	Seq   int64
	Entry ledger.Entry
}

type OutboxRepository interface {
	Append(ctx context.Context, evs ...events.Event) error
}

// InboxState is the state of an inbound message when the consumer starts
// handling it.
type InboxState struct {
	// New is true when this unit of work registered the message.
	New bool
	// HashMatches is false when the message id was seen with another payload.
	HashMatches bool
	// TransactionID is the operation the message produced, when completed.
	TransactionID uuid.UUID
	Completed     bool
}

type InboxRepository interface {
	Register(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (InboxState, error)
	Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error
}

// Repos are bound to one SQL transaction.
type Repos struct {
	Wallets      WalletRepository
	Transactions TransactionRepository
	Ledger       LedgerRepository
	Outbox       OutboxRepository
	Inbox        InboxRepository
}

// UnitOfWork delimits SQL transactions. Repositories never begin or commit.
type UnitOfWork interface {
	// Run executes fn in a READ COMMITTED transaction and commits if fn
	// returns nil. Errors are classified with ErrTransient, ErrIntegrity,
	// ErrDuplicate and friends.
	Run(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
	// Read executes fn in a REPEATABLE READ READ ONLY transaction: every
	// query sees the same snapshot.
	Read(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
}

// Recorder receives the business metrics. The adapter is Prometheus.
type Recorder interface {
	Operation(channel, kind, status string, replay bool, d time.Duration)
	IdempotencyConflict(channel string)
	LockConflict()
	PendingReferenceAttempt(outcome string)
	ReconciliationDivergence()
}
