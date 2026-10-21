package wallet_test

import (
	"errors"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func open(t *testing.T, initial string) wallet.Wallet {
	t.Helper()
	o, err := wallet.Open(uuid.New(), uuid.New(), brl(t, initial), uuid.New(), uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return o.Wallet
}

func TestOpenWithBalanceCreatesOpeningEntry(t *testing.T) {
	txID := uuid.New()
	o, err := wallet.Open(uuid.New(), uuid.New(), brl(t, "100.00"), txID, uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if o.Wallet.Version() != 1 || o.Wallet.Balance().Amount() != "100.00" {
		t.Fatalf("wallet = v%d %s", o.Wallet.Version(), o.Wallet.Balance())
	}
	e := o.Entry
	if e == nil || e.Direction() != ledger.Credit || e.BalanceBefore().Amount() != "0.00" ||
		e.BalanceAfter().Amount() != "100.00" || e.WalletVersion() != 1 || e.TransactionID() != txID {
		t.Fatalf("opening entry = %+v", e)
	}
}

func TestOpenWithZeroHasNoEntry(t *testing.T) {
	o, err := wallet.Open(uuid.New(), uuid.New(), brl(t, "0.00"), uuid.New(), uuid.New(), time.Now())
	if err != nil || o.Entry != nil || o.Wallet.Version() != 1 {
		t.Fatalf("open zero = %+v, %v", o, err)
	}
}

func TestOpenRejectsInvalidInput(t *testing.T) {
	for name, tc := range map[string]struct {
		id, player uuid.UUID
		m          money.Money
	}{
		"nil id":        {uuid.Nil, uuid.New(), brl(t, "1.00")},
		"nil player":    {uuid.New(), uuid.Nil, brl(t, "1.00")},
		"uninitialized": {uuid.New(), uuid.New(), money.Money{}},
		"negative":      {uuid.New(), uuid.New(), brl(t, "-1.00")},
	} {
		if _, err := wallet.Open(tc.id, tc.player, tc.m, uuid.New(), uuid.New(), time.Now()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestDebitKeepsBalanceNonNegative(t *testing.T) {
	w := open(t, "100.00")
	next, e, err := w.Apply(ledger.Debit, brl(t, "80.00"), uuid.New(), uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if next.Balance().Amount() != "20.00" || next.Version() != 2 || e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "20.00" {
		t.Fatalf("after debit: %s v%d, entry %s -> %s", next.Balance(), next.Version(), e.BalanceBefore(), e.BalanceAfter())
	}
	if w.Balance().Amount() != "100.00" || w.Version() != 1 {
		t.Fatal("Apply must not modify the receiver")
	}
	if _, _, err := next.Apply(ledger.Debit, brl(t, "80.00"), uuid.New(), uuid.New(), time.Now()); !errors.Is(err, wallet.ErrInsufficientFunds) {
		t.Fatalf("second debit err = %v", err)
	}
	if _, _, err := next.Apply(ledger.Debit, brl(t, "20.00"), uuid.New(), uuid.New(), time.Now()); err != nil {
		t.Fatalf("debit to exactly zero: %v", err)
	}
}

func TestApplyRejectsCurrencyMismatchAndNonPositive(t *testing.T) {
	w := open(t, "100.00")
	usd, _ := money.Parse("1.00", "USD")
	if _, _, err := w.Apply(ledger.Credit, usd, uuid.New(), uuid.New(), time.Now()); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch err = %v", err)
	}
	if _, _, err := w.Apply(ledger.Credit, brl(t, "0.00"), uuid.New(), uuid.New(), time.Now()); err == nil {
		t.Fatal("zero movement accepted")
	}
}

func TestCreditOverflow(t *testing.T) {
	max, _ := money.FromMinor(1<<63-1, money.MustCurrency("BRL"))
	w, err := wallet.Rehydrate(uuid.New(), uuid.New(), max, 7, time.Now(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Apply(ledger.Credit, brl(t, "0.01"), uuid.New(), uuid.New(), time.Now()); !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("overflow err = %v", err)
	}
}

func TestRehydrateValidatesWithoutChangingState(t *testing.T) {
	w, err := wallet.Rehydrate(uuid.New(), uuid.New(), brl(t, "5.00"), 9, time.Now(), time.Now())
	if err != nil || w.Version() != 9 || w.Balance().Amount() != "5.00" {
		t.Fatalf("rehydrate = %+v, %v", w, err)
	}
	if _, err := wallet.Rehydrate(uuid.New(), uuid.New(), brl(t, "-5.00"), 1, time.Now(), time.Now()); err == nil {
		t.Fatal("negative stored balance accepted")
	}
	if _, err := wallet.Rehydrate(uuid.New(), uuid.New(), brl(t, "5.00"), 0, time.Now(), time.Now()); err == nil {
		t.Fatal("version 0 accepted")
	}
}

func TestLedgerEntryArithmetic(t *testing.T) {
	ids := func() (uuid.UUID, uuid.UUID, uuid.UUID) { return uuid.New(), uuid.New(), uuid.New() }
	a, b, c := ids()
	if _, err := ledger.New(a, b, c, ledger.Credit, brl(t, "1.00"), brl(t, "0.00"), brl(t, "2.00"), 2, time.Now()); err == nil {
		t.Fatal("incoherent credit accepted")
	}
	if _, err := ledger.New(a, b, c, ledger.Debit, brl(t, "1.00"), brl(t, "0.50"), brl(t, "-0.50"), 2, time.Now()); err == nil {
		t.Fatal("negative balance accepted")
	}
	if _, err := ledger.New(a, b, c, "SIDEWAYS", brl(t, "1.00"), brl(t, "1.00"), brl(t, "2.00"), 2, time.Now()); err == nil {
		t.Fatal("unknown direction accepted")
	}
	if _, err := ledger.New(a, b, c, ledger.Debit, brl(t, "1.00"), brl(t, "3.00"), brl(t, "2.00"), 2, time.Now()); err != nil {
		t.Fatalf("valid debit: %v", err)
	}
}
