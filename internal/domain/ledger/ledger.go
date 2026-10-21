// Package ledger models the immutable wallet ledger entry.
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/google/uuid"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func ParseDirection(s string) (Direction, error) {
	switch Direction(s) {
	case Debit, Credit:
		return Direction(s), nil
	}
	return "", fmt.Errorf("%w: direction %q", ErrInvalidEntry, s)
}

// Opposite returns the direction that undoes d.
func (d Direction) Opposite() Direction {
	if d == Debit {
		return Credit
	}
	return Debit
}

var ErrInvalidEntry = errors.New("ledger: invalid entry")

// Entry is one append-only movement of one wallet. It has no setters: once
// built, it can only be read.
type Entry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int64
	createdAt     time.Time
}

// New validates balanceAfter = balanceBefore ± amount for the direction, a
// positive amount, non-negative balances and a single currency. It is used both
// to create an entry and to rehydrate one from storage; neither path changes
// any state, so the two share the same validation.
func New(id, walletID, transactionID uuid.UUID, dir Direction, amount, before, after money.Money, walletVersion int64, createdAt time.Time) (Entry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return Entry{}, fmt.Errorf("%w: missing identity", ErrInvalidEntry)
	}
	if _, err := ParseDirection(string(dir)); err != nil {
		return Entry{}, err
	}
	if !amount.IsValid() || !before.IsValid() || !after.IsValid() {
		return Entry{}, fmt.Errorf("%w: %w", ErrInvalidEntry, money.ErrUninitialized)
	}
	if !amount.IsPositive() {
		return Entry{}, fmt.Errorf("%w: amount must be positive", ErrInvalidEntry)
	}
	if before.IsNegative() || after.IsNegative() {
		return Entry{}, fmt.Errorf("%w: negative balance", ErrInvalidEntry)
	}
	var want money.Money
	var err error
	if dir == Credit {
		want, err = before.Add(amount)
	} else {
		want, err = before.Sub(amount)
	}
	if err != nil {
		return Entry{}, fmt.Errorf("%w: %w", ErrInvalidEntry, err)
	}
	if !want.Equal(after) {
		return Entry{}, fmt.Errorf("%w: %s %s %s != %s", ErrInvalidEntry, before, dir, amount, after)
	}
	if walletVersion < 1 {
		return Entry{}, fmt.Errorf("%w: wallet version %d", ErrInvalidEntry, walletVersion)
	}
	if createdAt.IsZero() {
		return Entry{}, fmt.Errorf("%w: missing timestamp", ErrInvalidEntry)
	}
	return Entry{id, walletID, transactionID, dir, amount, before, after, walletVersion, createdAt.UTC()}, nil
}

func (e Entry) ID() uuid.UUID              { return e.id }
func (e Entry) WalletID() uuid.UUID        { return e.walletID }
func (e Entry) TransactionID() uuid.UUID   { return e.transactionID }
func (e Entry) Direction() Direction       { return e.direction }
func (e Entry) Amount() money.Money        { return e.amount }
func (e Entry) BalanceBefore() money.Money { return e.balanceBefore }
func (e Entry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e Entry) WalletVersion() int64       { return e.walletVersion }
func (e Entry) CreatedAt() time.Time       { return e.createdAt }
