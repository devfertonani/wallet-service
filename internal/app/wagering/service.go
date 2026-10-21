package wagering

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/events"
	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
)

// Channels label where an operation came from, in metrics and logs.
const (
	ChannelHTTP   = "http"
	ChannelSQS    = "sqs"
	ChannelWorker = "worker"
)

type Service struct {
	uow    UnitOfWork
	rec    Recorder
	log    *slog.Logger
	policy wager.RetryPolicy
	now    func() time.Time
	newID  func() uuid.UUID
}

func NewService(uow UnitOfWork, rec Recorder, log *slog.Logger, policy wager.RetryPolicy) *Service {
	return &Service{uow: uow, rec: rec, log: log, policy: policy, now: time.Now, newID: newID}
}

// newID returns a UUIDv7: time ordered, so inserts stay at the right edge of
// the primary key index.
func newID() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}

// Source describes where an operation came from.
type Source struct {
	Channel       string
	CorrelationID string
	CausationID   string
	// Inbox is set for SQS: the message is registered and completed in the
	// same unit of work as the operation it produces.
	Inbox *InboxMessage
}

type InboxMessage struct {
	Consumer  string
	MessageID string
}

// Result is the persisted outcome of an operation. Replay is true when it was
// already recorded and nothing was applied now.
type Result struct {
	Transaction wager.Snapshot
	Replay      bool
}

// Submit processes an external operation, from HTTP or SQS, with the same
// guarantees for both: one effect per (provider, external id), replay of the
// original result for the same key and payload, conflict otherwise.
//
// Errors: *failure.Error (correctable input, e.g. WALLET_NOT_FOUND),
// ErrIdempotencyKeyReused, ErrDuplicateExternal, ErrMessageIDReused, or
// ErrTransient. A business rejection is not an error: it is a Result with
// status REJECTED.
func (s *Service) Submit(ctx context.Context, cmd wager.Command, src Source) (Result, error) {
	start := time.Now()
	res, err := s.submit(ctx, cmd, src)
	status := string(res.Transaction.Status)
	switch {
	case errors.Is(err, ErrIdempotencyKeyReused), errors.Is(err, ErrDuplicateExternal), errors.Is(err, ErrMessageIDReused):
		status = "CONFLICT"
		s.rec.IdempotencyConflict(src.Channel)
	case errors.Is(err, ErrTransient):
		status = "TRANSIENT"
		if errors.Is(err, ErrLockConflict) {
			s.rec.LockConflict()
		}
	case errors.Is(err, ErrIntegrity):
		status = "FAILED"
	case err != nil:
		status = "INVALID"
	}
	s.rec.Operation(src.Channel, string(cmd.Kind()), status, res.Replay, time.Since(start))
	return res, err
}

func (s *Service) submit(ctx context.Context, cmd wager.Command, src Source) (Result, error) {
	var res Result
	var err error
	// A concurrent copy of the same operation can commit between our lookup
	// and our insert; the unique index rejects ours, and the next attempt
	// reads the committed one and answers as a replay.
	for attempt := 0; attempt < 3; attempt++ {
		err = s.uow.Run(ctx, func(ctx context.Context, r Repos) error {
			var e error
			res, e = s.submitIn(ctx, r, cmd, src)
			return e
		})
		if !errors.Is(err, ErrDuplicate) {
			break
		}
	}
	switch {
	case errors.Is(err, ErrDuplicate), errors.Is(err, ErrConcurrentUpdate):
		s.rec.LockConflict()
		return Result{}, fmt.Errorf("%w: %w", ErrTransient, err)
	case errors.Is(err, ErrIntegrity):
		return s.recordFailure(ctx, cmd, src, err)
	}
	return res, err
}

func (s *Service) submitIn(ctx context.Context, r Repos, cmd wager.Command, src Source) (Result, error) {
	now := s.now()
	if src.Inbox != nil {
		st, err := r.Inbox.Register(ctx, src.Inbox.Consumer, src.Inbox.MessageID, cmd.PayloadHash(), now)
		if err != nil {
			return Result{}, err
		}
		if !st.HashMatches {
			return Result{}, ErrMessageIDReused
		}
		if st.Completed {
			t, err := r.Transactions.Get(ctx, st.TransactionID)
			if err != nil {
				return Result{}, err
			}
			return Result{Transaction: t.Snapshot(), Replay: true}, nil
		}
	}
	complete := func(res Result) (Result, error) {
		if src.Inbox != nil {
			if err := r.Inbox.Complete(ctx, src.Inbox.Consumer, src.Inbox.MessageID, res.Transaction.ID, now); err != nil {
				return Result{}, err
			}
		}
		return res, nil
	}

	// Replays are answered without taking the wallet lock.
	if res, found, err := s.existing(ctx, r, cmd); err != nil || found {
		if err != nil {
			return Result{}, err
		}
		return complete(res)
	}
	w, err := r.Wallets.GetForUpdate(ctx, cmd.WalletID())
	if errors.Is(err, ErrNotFound) {
		return Result{}, failure.New(failure.WalletNotFound, "wallet %s not found", cmd.WalletID())
	}
	if err != nil {
		return Result{}, err
	}
	// A copy that committed while we waited for the lock is visible now.
	if res, found, err := s.existing(ctx, r, cmd); err != nil || found {
		if err != nil {
			return Result{}, err
		}
		return complete(res)
	}

	t, err := wager.NewExternal(s.newID(), cmd, src.CorrelationID, now)
	if err != nil {
		return Result{}, err
	}
	ref, err := s.reference(ctx, r, t)
	if err != nil {
		return Result{}, err
	}
	out, err := wager.Settle(t, w, ref, s.policy, s.newID(), now)
	if err != nil {
		return Result{}, err
	}
	if err := r.Transactions.Insert(ctx, t); err != nil {
		return Result{}, err
	}
	meta := events.Meta{CorrelationID: src.CorrelationID, CausationID: src.CausationID}
	if err := s.persist(ctx, r, t, w.Version(), out, meta, true, now); err != nil {
		return Result{}, err
	}
	return complete(Result{Transaction: t.Snapshot()})
}

// existing looks the operation up by idempotency key and by external id.
func (s *Service) existing(ctx context.Context, r Repos, cmd wager.Command) (Result, bool, error) {
	t, err := r.Transactions.GetByIdempotencyKey(ctx, cmd.ProviderID(), cmd.IdempotencyKey())
	if errors.Is(err, ErrNotFound) {
		// Each statement sees its own snapshot under READ COMMITTED: a copy
		// that commits between the two lookups is found here, by external id,
		// with our own key. Only another key means a real duplicate.
		t, err = r.Transactions.GetByExternalID(ctx, cmd.ProviderID(), cmd.ExternalTransactionID())
		if err == nil && t.Snapshot().IdempotencyKey != cmd.IdempotencyKey() {
			return Result{}, false, ErrDuplicateExternal
		}
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return Result{}, false, nil
	case err != nil:
		return Result{}, false, err
	case t.PayloadHash() != cmd.PayloadHash():
		return Result{}, false, ErrIdempotencyKeyReused
	}
	return Result{Transaction: t.Snapshot(), Replay: true}, true, nil
}

// reference resolves (provider, referenceExternalTransactionId) inside the
// provider of the operation.
func (s *Service) reference(ctx context.Context, r Repos, t *wager.Transaction) (wager.Reference, error) {
	ext := t.Snapshot().ReferenceExternalTransactionID
	if ext == "" {
		return wager.Reference{}, nil
	}
	ref, err := r.Transactions.GetByExternalID(ctx, t.ProviderID(), ext)
	if errors.Is(err, ErrNotFound) {
		return wager.Reference{}, nil
	}
	if err != nil {
		return wager.Reference{}, err
	}
	reversed := false
	if t.Kind().RequiresReference() {
		if reversed, err = r.Transactions.IsReversed(ctx, ref.ID()); err != nil {
			return wager.Reference{}, err
		}
	}
	return wager.Reference{Transaction: ref, AlreadyReversed: reversed}, nil
}

// persist writes what settling produced, after the transaction row itself:
// ledger entry, wallet, outbox events and the wake-up of operations waiting
// for this one. Everything commits together or not at all.
func (s *Service) persist(ctx context.Context, r Repos, t *wager.Transaction, prevVersion int64, out wager.Outcome, meta events.Meta, fresh bool, now time.Time) error {
	snap := t.Snapshot()
	var evs []events.Event
	add := func(ev events.Event, err error) error {
		if err == nil {
			evs = append(evs, ev)
		}
		return err
	}
	switch snap.Status {
	case wager.Processed:
		if out.Entry != nil {
			if err := r.Ledger.Append(ctx, *out.Entry); err != nil {
				return err
			}
			if err := r.Wallets.Update(ctx, out.Wallet, prevVersion); err != nil {
				return err
			}
		}
		if err := add(events.NewProcessed(s.newID(), snap, meta, now)); err != nil {
			return err
		}
		if out.Entry != nil {
			if err := add(events.NewBalanceChanged(s.newID(), *out.Entry, meta, now)); err != nil {
				return err
			}
		}
		if snap.Kind == wager.Bet || snap.Kind == wager.Win || snap.Kind == wager.Refund {
			if err := r.Transactions.WakeWaiting(ctx, snap.ProviderID, snap.ExternalTransactionID, now); err != nil {
				return err
			}
		}
	case wager.Rejected:
		if err := add(events.NewRejected(s.newID(), snap, meta, now)); err != nil {
			return err
		}
	case wager.PendingReference:
		if fresh {
			if err := add(events.NewPendingReference(s.newID(), snap, meta, now)); err != nil {
				return err
			}
		}
	}
	return r.Outbox.Append(ctx, evs...)
}

// recordFailure registers FAILED / INVARIANT_VIOLATION for an operation the
// database refused although the domain accepted it, so it is auditable and is
// not retried forever. If writing it fails transiently, the error is
// transient; if the database refuses it too, the error stays ErrIntegrity.
func (s *Service) recordFailure(ctx context.Context, cmd wager.Command, src Source, cause error) (Result, error) {
	s.log.ErrorContext(ctx, "invariant violation, recording FAILED", "error", cause,
		"providerId", cmd.ProviderID(), "walletId", cmd.WalletID(), "correlationId", src.CorrelationID)
	var res Result
	err := s.uow.Run(ctx, func(ctx context.Context, r Repos) error {
		now := s.now()
		t, err := wager.NewExternal(s.newID(), cmd, src.CorrelationID, now)
		if err != nil {
			return err
		}
		if err := t.Fail(failure.InvariantViolation, now); err != nil {
			return err
		}
		if err := r.Transactions.Insert(ctx, t); err != nil {
			return err
		}
		if src.Inbox != nil {
			if _, err := r.Inbox.Register(ctx, src.Inbox.Consumer, src.Inbox.MessageID, cmd.PayloadHash(), now); err != nil {
				return err
			}
			if err := r.Inbox.Complete(ctx, src.Inbox.Consumer, src.Inbox.MessageID, t.ID(), now); err != nil {
				return err
			}
		}
		res = Result{Transaction: t.Snapshot()}
		return nil
	})
	switch {
	case errors.Is(err, ErrIntegrity):
		// Even the audit row is refused: retrying cannot help. Report it as
		// a permanent failure instead of looping as transient.
		return Result{}, fmt.Errorf("%w (recording FAILED also refused: %v)", cause, err)
	case err != nil:
		return Result{}, fmt.Errorf("%w: %w (recording failure: %v)", ErrTransient, cause, err)
	}
	return res, nil
}

// ResumePending retries the PENDING_REFERENCE operations that are due. Every
// instance runs it; each operation is settled under the lock of its wallet and
// its own row lock taken with SKIP LOCKED, so two workers never settle the
// same operation and never wait for each other. It returns how many it
// settled or rescheduled.
func (s *Service) ResumePending(ctx context.Context, limit int) (int, error) {
	var due []PendingRef
	err := s.uow.Read(ctx, func(ctx context.Context, r Repos) error {
		var err error
		due, err = r.Transactions.DuePending(ctx, s.now(), limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, p := range due {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		outcome, err := s.resumeOne(ctx, p)
		if errors.Is(err, ErrIntegrity) {
			outcome, err = s.failPending(ctx, p, err)
		}
		if err != nil {
			s.rec.PendingReferenceAttempt("error")
			s.log.WarnContext(ctx, "pending reference attempt failed", "transactionId", p.ID, "walletId", p.WalletID, "error", err)
			continue
		}
		if outcome != "" {
			s.rec.PendingReferenceAttempt(outcome)
			n++
		}
	}
	return n, nil
}

func (s *Service) resumeOne(ctx context.Context, p PendingRef) (string, error) {
	var outcome string
	err := s.uow.Run(ctx, func(ctx context.Context, r Repos) error {
		now := s.now()
		// Wallet first, then the operation: the same lock order as Submit.
		w, err := r.Wallets.GetForUpdate(ctx, p.WalletID)
		if err != nil {
			return err
		}
		t, err := r.Transactions.LockDuePending(ctx, p.ID, now)
		if err != nil || t == nil {
			return err
		}
		ref, err := s.reference(ctx, r, t)
		if err != nil {
			return err
		}
		out, err := wager.Settle(t, w, ref, s.policy, s.newID(), now)
		if err != nil {
			return err
		}
		if err := r.Transactions.Update(ctx, t); err != nil {
			return err
		}
		snap := t.Snapshot()
		meta := events.Meta{CorrelationID: snap.CorrelationID, CausationID: snap.ID.String()}
		if err := s.persist(ctx, r, t, w.Version(), out, meta, false, now); err != nil {
			return err
		}
		switch snap.Status {
		case wager.Processed:
			outcome = "processed"
		case wager.Rejected:
			outcome = "rejected"
		default:
			outcome = "rescheduled"
		}
		s.log.InfoContext(ctx, "pending reference attempt", "transactionId", snap.ID, "walletId", snap.WalletID,
			"providerId", snap.ProviderID, "correlationId", snap.CorrelationID, "outcome", outcome, "attempts", snap.Attempts)
		return nil
	})
	return outcome, err
}

func (s *Service) failPending(ctx context.Context, p PendingRef, cause error) (string, error) {
	s.log.ErrorContext(ctx, "invariant violation on pending reference, recording FAILED", "transactionId", p.ID, "error", cause)
	err := s.uow.Run(ctx, func(ctx context.Context, r Repos) error {
		now := s.now()
		if _, err := r.Wallets.GetForUpdate(ctx, p.WalletID); err != nil {
			return err
		}
		t, err := r.Transactions.LockDuePending(ctx, p.ID, now)
		if err != nil || t == nil {
			return err
		}
		if err := t.Fail(failure.InvariantViolation, now); err != nil {
			return err
		}
		return r.Transactions.Update(ctx, t)
	})
	return "failed", err
}

// OpenWallet opens a wallet. A positive initial balance creates the OPENING
// transaction, its credit and the outbox events in the same commit as the
// wallet; zero creates only the wallet.
func (s *Service) OpenWallet(ctx context.Context, playerID uuid.UUID, initial money.Money) (wallet.Wallet, error) {
	var w wallet.Wallet
	err := s.uow.Run(ctx, func(ctx context.Context, r Repos) error {
		now := s.now()
		walletID, txID := s.newID(), s.newID()
		o, err := wallet.Open(walletID, playerID, initial, txID, s.newID(), now)
		if err != nil {
			return err
		}
		if err := r.Wallets.Insert(ctx, o.Wallet); err != nil {
			return err
		}
		w = o.Wallet
		if o.Entry == nil {
			return nil
		}
		t, err := wager.NewOpening(txID, walletID, playerID, initial, now)
		if err != nil {
			return err
		}
		if err := r.Transactions.Insert(ctx, t); err != nil {
			return err
		}
		if err := r.Ledger.Append(ctx, *o.Entry); err != nil {
			return err
		}
		meta := events.Meta{CorrelationID: txID.String()}
		processed, err := events.NewProcessed(s.newID(), t.Snapshot(), meta, now)
		if err != nil {
			return err
		}
		changed, err := events.NewBalanceChanged(s.newID(), *o.Entry, meta, now)
		if err != nil {
			return err
		}
		return r.Outbox.Append(ctx, processed, changed)
	})
	if err != nil {
		return wallet.Wallet{}, err
	}
	s.log.InfoContext(ctx, "wallet opened", "walletId", w.ID(), "currency", w.Currency().String())
	return w, nil
}

func (s *Service) GetWallet(ctx context.Context, id uuid.UUID) (wallet.Wallet, error) {
	var w wallet.Wallet
	err := s.uow.Read(ctx, func(ctx context.Context, r Repos) error {
		var err error
		w, err = r.Wallets.Get(ctx, id)
		return err
	})
	return w, err
}

// LedgerPage returns entries of an existing wallet after the cursor.
func (s *Service) LedgerPage(ctx context.Context, walletID uuid.UUID, after int64, limit int) ([]LedgerRow, error) {
	var rows []LedgerRow
	err := s.uow.Read(ctx, func(ctx context.Context, r Repos) error {
		if _, err := r.Wallets.Get(ctx, walletID); err != nil {
			return err
		}
		var err error
		rows, err = r.Ledger.Page(ctx, walletID, after, limit)
		return err
	})
	return rows, err
}

func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := s.uow.Read(ctx, func(ctx context.Context, r Repos) error {
		t, err := r.Transactions.Get(ctx, id)
		if err == nil {
			snap = t.Snapshot()
		}
		return err
	})
	return snap, err
}

func (s *Service) GetByExternalID(ctx context.Context, providerID, externalID string) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := s.uow.Read(ctx, func(ctx context.Context, r Repos) error {
		t, err := r.Transactions.GetByExternalID(ctx, providerID, externalID)
		if err == nil {
			snap = t.Snapshot()
		}
		return err
	})
	return snap, err
}

type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile rebuilds the balance from the ledger and compares it with the
// stored one, both read from the same snapshot. It changes nothing.
func (s *Service) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var rec Reconciliation
	err := s.uow.Read(ctx, func(ctx context.Context, r Repos) error {
		w, err := r.Wallets.Get(ctx, walletID)
		if err != nil {
			return err
		}
		calculated, n, err := r.Ledger.Totals(ctx, walletID, w.Currency())
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		rec = Reconciliation{
			WalletID: walletID, StoredBalance: w.Balance(), CalculatedBalance: calculated,
			Difference: diff, Consistent: diff.IsZero(), CheckedEntries: n,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !rec.Consistent {
		s.rec.ReconciliationDivergence()
		s.log.ErrorContext(ctx, "reconciliation divergence", "walletId", walletID,
			"storedBalance", rec.StoredBalance.Amount(), "calculatedBalance", rec.CalculatedBalance.Amount(),
			"difference", rec.Difference.Amount(), "checkedEntries", rec.CheckedEntries)
	}
	return rec, nil
}
