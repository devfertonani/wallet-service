// Package wallet models the wallet aggregate: the only place where a balance
// changes, always together with the ledger entry that explains the change.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/google/uuid"
)

var (
	ErrInvalidWallet     = errors.New("wallet: invalid wallet")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
)

// InitialVersion is the version of a freshly opened wallet. The version only
// increases when the balance changes.
const InitialVersion int64 = 1

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Opening is the result of opening a wallet. With a positive initial balance
// it carries the OPENING ledger entry, which must be committed together with
// the wallet; with zero there is no entry.
type Opening struct {
	Wallet Wallet
	Entry  *ledger.Entry
}

// Open creates a wallet at version 1. A positive initial balance is recorded
// as a credit from zero, linked to openingTxID.
func Open(id, playerID uuid.UUID, initial money.Money, openingTxID, entryID uuid.UUID, now time.Time) (Opening, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return Opening{}, fmt.Errorf("%w: missing identity", ErrInvalidWallet)
	}
	if !initial.IsValid() {
		return Opening{}, fmt.Errorf("%w: %w", ErrInvalidWallet, money.ErrUninitialized)
	}
	if initial.IsNegative() {
		return Opening{}, fmt.Errorf("%w: %w", ErrInvalidWallet, money.ErrNegativeAmount)
	}
	now = now.UTC()
	w := Wallet{id: id, playerID: playerID, balance: initial, version: InitialVersion, createdAt: now, updatedAt: now}
	if initial.IsZero() {
		return Opening{Wallet: w}, nil
	}
	zero, _ := money.Zero(initial.Currency())
	e, err := ledger.New(entryID, id, openingTxID, ledger.Credit, initial, zero, initial, InitialVersion, now)
	if err != nil {
		return Opening{}, err
	}
	return Opening{Wallet: w, Entry: &e}, nil
}

// Rehydrate rebuilds a wallet read from storage. It validates the state but
// applies no movement and does not touch the version.
func Rehydrate(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil || !balance.IsValid() || balance.IsNegative() || version < InitialVersion {
		return Wallet{}, fmt.Errorf("%w: inconsistent stored state for %s", ErrInvalidWallet, id)
	}
	return Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

// Apply moves the balance in the given direction and returns the new state and
// its ledger entry. The receiver is not modified. A debit that would leave the
// balance below zero fails with ErrInsufficientFunds.
func (w Wallet) Apply(dir ledger.Direction, amount money.Money, transactionID, entryID uuid.UUID, now time.Time) (Wallet, ledger.Entry, error) {
	if !amount.IsPositive() {
		return Wallet{}, ledger.Entry{}, fmt.Errorf("%w: movement amount must be positive", ErrInvalidWallet)
	}
	if amount.Currency() != w.Currency() {
		return Wallet{}, ledger.Entry{}, fmt.Errorf("%w: wallet %s", money.ErrCurrencyMismatch, w.Currency())
	}
	var after money.Money
	var err error
	switch dir {
	case ledger.Credit:
		after, err = w.balance.Add(amount)
	case ledger.Debit:
		after, err = w.balance.Sub(amount)
		if err == nil && after.IsNegative() {
			return Wallet{}, ledger.Entry{}, ErrInsufficientFunds
		}
	default:
		err = fmt.Errorf("%w: direction %q", ErrInvalidWallet, dir)
	}
	if err != nil {
		return Wallet{}, ledger.Entry{}, err
	}
	next := w
	next.balance = after
	next.version = w.version + 1
	next.updatedAt = now.UTC()
	e, err := ledger.New(entryID, w.id, transactionID, dir, amount, w.balance, after, next.version, now)
	if err != nil {
		return Wallet{}, ledger.Entry{}, err
	}
	return next, e, nil
}

func (w Wallet) ID() uuid.UUID            { return w.id }
func (w Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w Wallet) Balance() money.Money     { return w.balance }
func (w Wallet) Currency() money.Currency { return w.balance.Currency() }
func (w Wallet) Version() int64           { return w.version }
func (w Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w Wallet) UpdatedAt() time.Time     { return w.updatedAt }
