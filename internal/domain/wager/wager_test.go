package wager_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/ledger"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/google/uuid"
)

var (
	walletID = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
	playerID = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	policy   = wager.RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, TTL: 10 * time.Minute}
)

func input(kind, amount string) wager.CommandInput {
	return wager.CommandInput{
		ProviderID: "provider-a", ExternalTransactionID: "tx-" + kind + "-" + amount, IdempotencyKey: "provider-a:tx-" + kind,
		PlayerID: playerID.String(), WalletID: walletID.String(), RoundID: "round-1", GameID: "fortune-chimp",
		Kind: kind, Amount: amount, Currency: "BRL",
	}
}

func command(t *testing.T, in wager.CommandInput) wager.Command {
	t.Helper()
	c, err := wager.NewCommand(in)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTx(t *testing.T, in wager.CommandInput) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(uuid.New(), command(t, in), "corr", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func walletWith(t *testing.T, amount string) wallet.Wallet {
	t.Helper()
	m, _ := money.Parse(amount, "BRL")
	w, err := wallet.Rehydrate(walletID, playerID, m, 3, time.Now(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// processed returns a PROCESSED transaction of the kind, usable as a reference.
func processed(t *testing.T, kind, amount, extID string) *wager.Transaction {
	t.Helper()
	in := input(kind, amount)
	in.ExternalTransactionID = extID
	if kind == "REFUND" || kind == "ROLLBACK" {
		in.ReferenceExternalTransactionID = "whatever"
	}
	tx := newTx(t, in)
	if err := tx.Process(walletWith(t, "1.00").Balance(), uuid.Nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	return tx
}

func codeOf(t *testing.T, tx *wager.Transaction) failure.Code {
	t.Helper()
	if tx.Status() != wager.Rejected {
		t.Fatalf("status = %s, want REJECTED", tx.Status())
	}
	return tx.FailureCode()
}

func TestCommandValidation(t *testing.T) {
	cases := map[string]struct {
		mutate func(*wager.CommandInput)
		code   failure.Code
	}{
		"opening is internal":  {func(in *wager.CommandInput) { in.Kind = "OPENING" }, failure.UnsupportedKind},
		"unknown kind":         {func(in *wager.CommandInput) { in.Kind = "JACKPOT" }, failure.UnsupportedKind},
		"lowercase kind":       {func(in *wager.CommandInput) { in.Kind = "bet" }, failure.UnsupportedKind},
		"zero bet":             {func(in *wager.CommandInput) { in.Amount = "0.00" }, failure.InvalidAmount},
		"negative bet":         {func(in *wager.CommandInput) { in.Amount = "-1.00" }, failure.InvalidAmount},
		"scale":                {func(in *wager.CommandInput) { in.Amount = "1.005" }, failure.InvalidAmount},
		"scientific":           {func(in *wager.CommandInput) { in.Amount = "1e2" }, failure.InvalidAmount},
		"NaN":                  {func(in *wager.CommandInput) { in.Amount = "NaN" }, failure.InvalidAmount},
		"empty amount":         {func(in *wager.CommandInput) { in.Amount = "" }, failure.InvalidAmount},
		"overflow":             {func(in *wager.CommandInput) { in.Amount = "92233720368547758.08" }, failure.InvalidAmount},
		"currency":             {func(in *wager.CommandInput) { in.Currency = "JPY" }, failure.InvalidCurrency},
		"player":               {func(in *wager.CommandInput) { in.PlayerID = "not-a-uuid" }, failure.InvalidRequest},
		"wallet":               {func(in *wager.CommandInput) { in.WalletID = "" }, failure.InvalidRequest},
		"provider":             {func(in *wager.CommandInput) { in.ProviderID = "" }, failure.InvalidRequest},
		"key":                  {func(in *wager.CommandInput) { in.IdempotencyKey = "" }, failure.InvalidRequest},
		"round":                {func(in *wager.CommandInput) { in.RoundID = "" }, failure.InvalidRequest},
		"game":                 {func(in *wager.CommandInput) { in.GameID = "with space" }, failure.InvalidRequest},
		"long id":              {func(in *wager.CommandInput) { in.ExternalTransactionID = strings.Repeat("x", 129) }, failure.InvalidRequest},
		"refund without ref":   {func(in *wager.CommandInput) { in.Kind = "REFUND" }, failure.MissingReference},
		"rollback without ref": {func(in *wager.CommandInput) { in.Kind = "ROLLBACK" }, failure.MissingReference},
		"bet with ref":         {func(in *wager.CommandInput) { in.ReferenceExternalTransactionID = "x" }, failure.InvalidRequest},
		"loss with positive":   {func(in *wager.CommandInput) { in.Kind = "LOSS" }, failure.InvalidAmount},
		"loss with ref": {func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "LOSS", "0.00", "x"
		}, failure.InvalidRequest},
		"zero win": {func(in *wager.CommandInput) { in.Kind, in.Amount = "WIN", "0.00" }, failure.InvalidAmount},
		"zero refund": {func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "REFUND", "0.00", "x"
		}, failure.InvalidAmount},
		"zero rollback": {func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "ROLLBACK", "0.00", "x"
		}, failure.InvalidAmount},
	}
	for name, tc := range cases {
		in := input("BET", "25.00")
		tc.mutate(&in)
		_, err := wager.NewCommand(in)
		if !failure.Is(err, tc.code) {
			t.Errorf("%s: err = %v, want %s", name, err, tc.code)
		}
	}
	loss := input("LOSS", "0.00")
	if _, err := wager.NewCommand(loss); err != nil {
		t.Errorf("LOSS 0.00 rejected: %v", err)
	}
}

func TestPayloadHash(t *testing.T) {
	base := command(t, input("BET", "25.00"))
	if !strings.HasPrefix(base.PayloadHash(), wager.HashVersion) {
		t.Fatalf("hash %q lacks version prefix", base.PayloadHash())
	}

	// The idempotency key is transport, not business content.
	otherKey := input("BET", "25.00")
	otherKey.IdempotencyKey = "something-else"
	if command(t, otherKey).PayloadHash() != base.PayloadHash() {
		t.Error("idempotency key changed the hash")
	}

	// UUIDs are normalized before hashing.
	upper := input("BET", "25.00")
	upper.PlayerID = strings.ToUpper(upper.PlayerID)
	if command(t, upper).PayloadHash() != base.PayloadHash() {
		t.Error("UUID case changed the hash")
	}

	for name, mutate := range map[string]func(*wager.CommandInput){
		"amount":   func(in *wager.CommandInput) { in.Amount = "25.01" },
		"currency": func(in *wager.CommandInput) { in.Currency = "USD" },
		"round":    func(in *wager.CommandInput) { in.RoundID = "round-2" },
		"game":     func(in *wager.CommandInput) { in.GameID = "other" },
		"wallet":   func(in *wager.CommandInput) { in.WalletID = uuid.NewString() },
	} {
		in := input("BET", "25.00")
		mutate(&in)
		if command(t, in).PayloadHash() == base.PayloadHash() {
			t.Errorf("%s did not change the hash", name)
		}
	}
}

func TestCanonicalJSONSortsKeysWithoutEscaping(t *testing.T) {
	got := string(wager.CanonicalJSON(map[string]any{"b": "<x>", "a": map[string]string{"z": "1", "y": "2"}}))
	if want := `{"a":{"y":"2","z":"1"},"b":"<x>"}`; got != want {
		t.Fatalf("canonical = %s, want %s", got, want)
	}
}

func TestStateMachine(t *testing.T) {
	tx := newTx(t, input("BET", "25.00"))
	if tx.Status() != wager.Pending {
		t.Fatalf("initial status = %s", tx.Status())
	}
	bal := walletWith(t, "1.00").Balance()
	if err := tx.Process(bal, uuid.Nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func() error{
		"process": func() error { return tx.Process(bal, uuid.Nil, time.Now()) },
		"reject":  func() error { return tx.Reject(failure.InsufficientFunds, nil, time.Now()) },
		"fail":    func() error { return tx.Fail(failure.InvariantViolation, time.Now()) },
		"await": func() error {
			return tx.AwaitReference(time.Now().Add(time.Second), time.Now().Add(time.Hour), time.Now())
		},
		"reschedule": func() error { return tx.Reschedule(time.Now(), time.Now()) },
	} {
		if err := f(); !errors.Is(err, wager.ErrInvalidTransition) {
			t.Errorf("%s from PROCESSED: err = %v", name, err)
		}
	}

	pending := newTx(t, input("BET", "25.00"))
	if err := pending.AwaitReference(time.Now().Add(time.Second), time.Now().Add(time.Hour), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := pending.AwaitReference(time.Now().Add(time.Second), time.Now().Add(time.Hour), time.Now()); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Errorf("PENDING_REFERENCE -> PENDING_REFERENCE: %v", err)
	}
	if err := pending.Reschedule(time.Now().Add(time.Second), time.Now()); err != nil || pending.Snapshot().Attempts != 1 {
		t.Errorf("reschedule: %v, attempts %d", err, pending.Snapshot().Attempts)
	}
	if err := pending.Reject(failure.ReferenceNotFound, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if s := pending.Snapshot(); s.NextAttemptAt != nil || s.ExpiresAt != nil {
		t.Error("terminal transaction kept its retry schedule")
	}

	if err := newTx(t, input("BET", "25.00")).Reject(failure.InvalidAmount, nil, time.Now()); err == nil {
		t.Error("a correctable code was accepted as a persisted rejection")
	}
}

func TestOpening(t *testing.T) {
	amount, _ := money.Parse("1000.00", "BRL")
	tx, err := wager.NewOpening(uuid.New(), walletID, playerID, amount, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	if s.Kind != wager.Opening || s.Origin != wager.Internal || s.Status != wager.Processed ||
		s.ProviderID != "" || s.ExternalTransactionID != "" || s.IdempotencyKey != "" || s.PayloadHash != "" ||
		s.RoundID != "" || s.GameID != "" || s.ResultBalance == nil || s.ResultBalance.Amount() != "1000.00" {
		t.Fatalf("opening snapshot = %+v", s)
	}
	zero, _ := money.Parse("0.00", "BRL")
	if _, err := wager.NewOpening(uuid.New(), walletID, playerID, zero, time.Now()); err == nil {
		t.Fatal("zero OPENING accepted")
	}
}

func TestRehydrateDoesNotReplayTransitions(t *testing.T) {
	tx := processed(t, "BET", "10.00", "bet-1")
	s := tx.Snapshot()
	back, err := wager.Rehydrate(s)
	if err != nil {
		t.Fatal(err)
	}
	if back.Status() != wager.Processed || back.Snapshot().UpdatedAt != s.UpdatedAt {
		t.Fatal("rehydration changed the state")
	}
	s.ResultBalance = nil
	if _, err := wager.Rehydrate(s); err == nil {
		t.Fatal("processed without result balance rehydrated")
	}
}

func settle(t *testing.T, tx *wager.Transaction, w wallet.Wallet, ref wager.Reference) wager.Outcome {
	t.Helper()
	out, err := wager.Settle(tx, w, ref, policy, uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSettleBet(t *testing.T) {
	tx := newTx(t, input("BET", "80.00"))
	out := settle(t, tx, walletWith(t, "100.00"), wager.Reference{})
	if tx.Status() != wager.Processed || out.Entry == nil || out.Entry.Direction() != ledger.Debit || out.Wallet.Balance().Amount() != "20.00" {
		t.Fatalf("bet: %s %+v", tx.Status(), out)
	}
	if out.Wallet.Version() != 4 || tx.Snapshot().ResultBalance.Amount() != "20.00" {
		t.Fatalf("version %d result %s", out.Wallet.Version(), tx.Snapshot().ResultBalance)
	}

	second := newTx(t, input("BET", "80.00"))
	out = settle(t, second, out.Wallet, wager.Reference{})
	if codeOf(t, second) != failure.InsufficientFunds || out.Entry != nil {
		t.Fatalf("second bet: %+v", out)
	}
}

func TestSettleLossHasNoMovement(t *testing.T) {
	tx := newTx(t, input("LOSS", "0.00"))
	w := walletWith(t, "5.00")
	out := settle(t, tx, w, wager.Reference{})
	if tx.Status() != wager.Processed || out.Entry != nil || out.Wallet.Version() != w.Version() {
		t.Fatalf("loss: %s %+v", tx.Status(), out)
	}
}

func TestSettleRejectsWalletMismatches(t *testing.T) {
	in := input("BET", "1.00")
	in.PlayerID = uuid.NewString()
	tx := newTx(t, in)
	settle(t, tx, walletWith(t, "5.00"), wager.Reference{})
	if codeOf(t, tx) != failure.PlayerWalletMismatch {
		t.Fatal(tx.FailureCode())
	}

	in = input("BET", "1.00")
	in.Currency = "USD"
	tx = newTx(t, in)
	settle(t, tx, walletWith(t, "5.00"), wager.Reference{})
	if codeOf(t, tx) != failure.CurrencyMismatch {
		t.Fatal(tx.FailureCode())
	}
}

func reversal(kind, amount, ref string) wager.CommandInput {
	in := input(kind, amount)
	in.ReferenceExternalTransactionID = ref
	return in
}

func TestSettleReversals(t *testing.T) {
	bet := processed(t, "BET", "10.00", "bet-1")
	win := processed(t, "WIN", "10.00", "win-1")
	refund := processed(t, "REFUND", "10.00", "refund-1")
	loss := processed(t, "LOSS", "0.00", "loss-1")
	rejected := newTx(t, input("BET", "10.00"))
	_ = rejected.Reject(failure.InsufficientFunds, nil, time.Now())

	cases := []struct {
		name string
		in   wager.CommandInput
		ref  wager.Reference
		w    string
		dir  ledger.Direction
		code failure.Code
	}{
		{"refund of bet credits", reversal("REFUND", "10.00", "bet-1"), wager.Reference{Transaction: bet}, "0.00", ledger.Credit, ""},
		{"rollback of bet credits", reversal("ROLLBACK", "10.00", "bet-1"), wager.Reference{Transaction: bet}, "0.00", ledger.Credit, ""},
		{"rollback of win debits", reversal("ROLLBACK", "10.00", "win-1"), wager.Reference{Transaction: win}, "10.00", ledger.Debit, ""},
		{"rollback of refund debits", reversal("ROLLBACK", "10.00", "refund-1"), wager.Reference{Transaction: refund}, "10.00", ledger.Debit, ""},
		{"win referencing bet", reversal("WIN", "50.00", "bet-1"), wager.Reference{Transaction: bet}, "0.00", ledger.Credit, ""},
		{"rollback of win without funds", reversal("ROLLBACK", "10.00", "win-1"), wager.Reference{Transaction: win}, "9.99", "", failure.ReversalInsufficientFunds},
		{"refund of win", reversal("REFUND", "10.00", "win-1"), wager.Reference{Transaction: win}, "0.00", "", failure.InvalidReferenceKind},
		{"rollback of loss", reversal("ROLLBACK", "10.00", "loss-1"), wager.Reference{Transaction: loss}, "0.00", "", failure.InvalidReferenceKind},
		{"win referencing win", reversal("WIN", "10.00", "win-1"), wager.Reference{Transaction: win}, "0.00", "", failure.InvalidReferenceKind},
		{"partial refund", reversal("REFUND", "5.00", "bet-1"), wager.Reference{Transaction: bet}, "0.00", "", failure.AmountMismatch},
		{"double refund", reversal("REFUND", "10.00", "bet-1"), wager.Reference{Transaction: bet, AlreadyReversed: true}, "0.00", "", failure.AlreadyReversed},
		{"rollback after refund", reversal("ROLLBACK", "10.00", "bet-1"), wager.Reference{Transaction: bet, AlreadyReversed: true}, "0.00", "", failure.AlreadyReversed},
		{"reference rejected", reversal("REFUND", "10.00", "x"), wager.Reference{Transaction: rejected}, "0.00", "", failure.ReferenceNotProcessed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := newTx(t, tc.in)
			out := settle(t, tx, walletWith(t, tc.w), tc.ref)
			if tc.code != "" {
				if codeOf(t, tx) != tc.code {
					t.Fatalf("code = %s, want %s", tx.FailureCode(), tc.code)
				}
				return
			}
			if tx.Status() != wager.Processed || out.Entry == nil || out.Entry.Direction() != tc.dir {
				t.Fatalf("status %s entry %+v", tx.Status(), out.Entry)
			}
			if tx.Snapshot().ReferenceTransactionID != tc.ref.Transaction.ID() {
				t.Fatal("reference not resolved")
			}
		})
	}
}

func TestSettleReferenceMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*wager.CommandInput){
		"round":    func(in *wager.CommandInput) { in.RoundID = "other-round" },
		"player":   func(in *wager.CommandInput) { in.PlayerID = uuid.NewString() },
		"wallet":   func(in *wager.CommandInput) { in.WalletID = uuid.NewString() },
		"currency": func(in *wager.CommandInput) { in.Currency = "USD" },
	} {
		in := input("BET", "10.00")
		mutate(&in)
		bet := newTx(t, in)
		result, _ := money.Parse("1.00", in.Currency)
		if err := bet.Process(result, uuid.Nil, time.Now()); err != nil {
			t.Fatal(err)
		}
		tx := newTx(t, reversal("REFUND", "10.00", in.ExternalTransactionID))
		settle(t, tx, walletWith(t, "0.00"), wager.Reference{Transaction: bet})
		if codeOf(t, tx) != failure.ReferenceMismatch {
			t.Errorf("%s: code = %s", name, tx.FailureCode())
		}
	}
}

func TestSettleMissingReferenceWaitsThenExpires(t *testing.T) {
	tx := newTx(t, reversal("REFUND", "10.00", "bet-late"))
	w := walletWith(t, "0.00")
	start := time.Now()
	if _, err := wager.Settle(tx, w, wager.Reference{}, policy, uuid.New(), start); err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	if tx.Status() != wager.PendingReference || s.NextAttemptAt == nil || s.ExpiresAt == nil || !s.ExpiresAt.Equal(start.Add(policy.TTL).UTC()) {
		t.Fatalf("pending: %+v", s)
	}

	// A reference that exists but is itself pending is still unavailable.
	pendingBet := newTx(t, input("BET", "10.00"))
	_ = pendingBet.AwaitReference(start.Add(time.Second), start.Add(time.Hour), start)
	if _, err := wager.Settle(tx, w, wager.Reference{Transaction: pendingBet}, policy, uuid.New(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != wager.PendingReference || tx.Snapshot().Attempts != 1 {
		t.Fatalf("retry: %s attempts %d", tx.Status(), tx.Snapshot().Attempts)
	}

	if _, err := wager.Settle(tx, w, wager.Reference{}, policy, uuid.New(), start.Add(policy.TTL)); err != nil {
		t.Fatal(err)
	}
	if codeOf(t, tx) != failure.ReferenceNotFound {
		t.Fatal(tx.FailureCode())
	}
}

func TestSettleLateReferenceResolves(t *testing.T) {
	tx := newTx(t, reversal("ROLLBACK", "10.00", "bet-1"))
	now := time.Now()
	_, _ = wager.Settle(tx, walletWith(t, "0.00"), wager.Reference{}, policy, uuid.New(), now)
	bet := processed(t, "BET", "10.00", "bet-1")
	out, err := wager.Settle(tx, walletWith(t, "0.00"), wager.Reference{Transaction: bet}, policy, uuid.New(), now.Add(2*time.Second))
	if err != nil || tx.Status() != wager.Processed || out.Entry == nil || out.Entry.Direction() != ledger.Credit {
		t.Fatalf("late resolution: %v %s %+v", err, tx.Status(), out)
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	for attempts := 0; attempts < 20; attempts++ {
		d := policy.Delay(attempts)
		if d <= 0 || d > policy.MaxDelay {
			t.Fatalf("delay(%d) = %s", attempts, d)
		}
	}
	if d := policy.Delay(0); d < 800*time.Millisecond || d > 1200*time.Millisecond {
		t.Fatalf("delay(0) = %s, want 1s ± 20%%", d)
	}
}
