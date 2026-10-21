package wager

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
)

// Reference is what the caller found for (providerId, referenceExternalTransactionId)
// before settling. A nil Transaction means it has not arrived yet.
type Reference struct {
	Transaction *Transaction
	// AlreadyReversed is true when a REFUND or ROLLBACK of this reference has
	// already been processed.
	AlreadyReversed bool
}

// RetryPolicy is the backoff of operations waiting for a reference.
type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	TTL       time.Duration
}

// Delay returns min(base × 2^attempts, max) with ±20% jitter, so instances
// that retry together drift apart.
func (p RetryPolicy) Delay(attempts int) time.Duration {
	d := p.BaseDelay
	for i := 0; i < attempts && d < p.MaxDelay; i++ {
		d *= 2
	}
	d = min(d, p.MaxDelay)
	jitter := time.Duration(rand.Int64N(int64(d)/5 + 1))
	if rand.IntN(2) == 0 {
		return d - jitter
	}
	return min(d+jitter, p.MaxDelay)
}

// Outcome is what settling produced. When Entry is set, Wallet is the new
// state and both must be committed together with the transaction.
type Outcome struct {
	Wallet wallet.Wallet
	Entry  *ledger.Entry
	// ResolvedReference is the reference that became available, if any.
	ResolvedReference uuid.UUID
}

// errUnavailable marks a reference that has not arrived or is still pending.
var errUnavailable = errors.New("wager: reference unavailable")

// Settle decides the operation against the current wallet (read under its row
// lock) and the reference, and moves the transaction to its next state:
// PROCESSED with a ledger entry when money moves, REJECTED with a failure code,
// or PENDING_REFERENCE (rescheduled, or rejected once the TTL has passed) when
// the reference is not available yet.
func Settle(t *Transaction, w wallet.Wallet, ref Reference, policy RetryPolicy, entryID uuid.UUID, now time.Time) (Outcome, error) {
	if t.s.Status.Terminal() {
		return Outcome{}, ErrInvalidTransition
	}
	observed := w.Balance()
	reject := func(code failure.Code) (Outcome, error) {
		return Outcome{Wallet: w}, t.Reject(code, &observed, now)
	}

	if t.s.WalletID != w.ID() {
		return Outcome{}, ErrInvalidTransaction
	}
	// The caller is not acting for this wallet's player, or not in its
	// currency: reject without recording or revealing the balance.
	if t.s.PlayerID != w.PlayerID() {
		return Outcome{Wallet: w}, t.Reject(failure.PlayerWalletMismatch, nil, now)
	}
	if t.s.Money.Currency() != w.Currency() {
		return Outcome{Wallet: w}, t.Reject(failure.CurrencyMismatch, nil, now)
	}

	dir, refID, code, err := t.plan(ref)
	switch {
	case errors.Is(err, errUnavailable):
		return t.await(w, policy, now)
	case code != "":
		return reject(code)
	case err != nil:
		return Outcome{}, err
	}

	if dir == "" { // LOSS: processed with no movement and no version change
		return Outcome{Wallet: w}, t.Process(observed, refID, now)
	}
	next, entry, err := w.Apply(dir, t.s.Money, t.s.ID, entryID, now)
	switch {
	case errors.Is(err, wallet.ErrInsufficientFunds) && t.s.Kind == Rollback:
		return reject(failure.ReversalInsufficientFunds)
	case errors.Is(err, wallet.ErrInsufficientFunds):
		return reject(failure.InsufficientFunds)
	case errors.Is(err, money.ErrOverflow):
		return reject(failure.BalanceLimitExceeded)
	}
	if err != nil {
		return Outcome{}, err
	}
	if err := t.Process(next.Balance(), refID, now); err != nil {
		return Outcome{}, err
	}
	return Outcome{Wallet: next, Entry: &entry, ResolvedReference: refID}, nil
}

// plan returns the movement of the kind, the resolved reference, or the
// rejection code. An empty direction with no code means no movement (LOSS).
func (t *Transaction) plan(ref Reference) (ledger.Direction, uuid.UUID, failure.Code, error) {
	switch t.s.Kind {
	case Bet:
		return ledger.Debit, uuid.Nil, "", nil
	case Loss:
		return "", uuid.Nil, "", nil
	case Win:
		if t.s.ReferenceExternalTransactionID == "" {
			return ledger.Credit, uuid.Nil, "", nil
		}
		r, code, err := t.resolve(ref, Bet)
		if err != nil || code != "" {
			return "", uuid.Nil, code, err
		}
		return ledger.Credit, r.s.ID, "", nil
	case Refund:
		r, code, err := t.resolve(ref, Bet)
		if err != nil || code != "" {
			return "", uuid.Nil, code, err
		}
		if code := t.reversible(r, ref); code != "" {
			return "", uuid.Nil, code, nil
		}
		return ledger.Credit, r.s.ID, "", nil
	case Rollback:
		r, code, err := t.resolve(ref, Bet, Win, Refund)
		if err != nil || code != "" {
			return "", uuid.Nil, code, err
		}
		if code := t.reversible(r, ref); code != "" {
			return "", uuid.Nil, code, nil
		}
		// Undo the original movement: BET was a debit, WIN and REFUND credits.
		if r.s.Kind == Bet {
			return ledger.Credit, r.s.ID, "", nil
		}
		return ledger.Debit, r.s.ID, "", nil
	}
	return "", uuid.Nil, "", ErrInvalidTransaction
}

func (t *Transaction) resolve(ref Reference, allowed ...Kind) (*Transaction, failure.Code, error) {
	r := ref.Transaction
	if r == nil || r.s.Status == Pending || r.s.Status == PendingReference {
		return nil, "", errUnavailable
	}
	if r.s.Status != Processed {
		return nil, failure.ReferenceNotProcessed, nil
	}
	kindOK := false
	for _, k := range allowed {
		kindOK = kindOK || r.s.Kind == k
	}
	if !kindOK {
		return nil, failure.InvalidReferenceKind, nil
	}
	if r.s.ProviderID != t.s.ProviderID || r.s.PlayerID != t.s.PlayerID || r.s.WalletID != t.s.WalletID ||
		r.s.Money.Currency() != t.s.Money.Currency() || r.s.RoundID != t.s.RoundID {
		return nil, failure.ReferenceMismatch, nil
	}
	return r, "", nil
}

// reversible applies the rules shared by REFUND and ROLLBACK: full amount
// only, and a reference is compensated at most once with success.
func (t *Transaction) reversible(r *Transaction, ref Reference) failure.Code {
	if !r.s.Money.Equal(t.s.Money) {
		return failure.AmountMismatch
	}
	if ref.AlreadyReversed {
		return failure.AlreadyReversed
	}
	return ""
}

func (t *Transaction) await(w wallet.Wallet, p RetryPolicy, now time.Time) (Outcome, error) {
	switch t.s.Status {
	case Pending:
		return Outcome{Wallet: w}, t.AwaitReference(now.Add(p.Delay(0)), now.Add(p.TTL), now)
	case PendingReference:
		if !now.Before(*t.s.ExpiresAt) {
			observed := w.Balance()
			return Outcome{Wallet: w}, t.Reject(failure.ReferenceNotFound, &observed, now)
		}
		return Outcome{Wallet: w}, t.Reschedule(now.Add(p.Delay(t.s.Attempts+1)), now)
	}
	return Outcome{}, ErrInvalidTransition
}
