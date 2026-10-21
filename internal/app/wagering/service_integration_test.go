//go:build integration

package wagering_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/adapters/pgstore"
	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/testsupport/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type noopRecorder struct{}

func (noopRecorder) Operation(string, string, string, bool, time.Duration) {}
func (noopRecorder) IdempotencyConflict(string)                            {}
func (noopRecorder) LockConflict()                                         {}
func (noopRecorder) PendingReferenceAttempt(string)                        {}
func (noopRecorder) ReconciliationDivergence()                             {}

var fastRetry = wager.RetryPolicy{BaseDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond, TTL: 2 * time.Second}

type env struct {
	t   *testing.T
	db  *pgtest.DB
	svc *wagering.Service
}

func setup(t *testing.T) *env {
	t.Helper()
	db := pgtest.New(t)
	return &env{t: t, db: db, svc: newService(db.App)}
}

// newService builds an independent service over its own pool, the way each
// instance of the process does.
func newService(pool *pgxpool.Pool) *wagering.Service {
	return wagering.NewService(pgstore.NewUnitOfWork(pool), noopRecorder{}, slog.New(slog.NewTextHandler(io.Discard, nil)), fastRetry)
}

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (e *env) open(initial string) (walletID, playerID uuid.UUID) {
	e.t.Helper()
	playerID = uuid.New()
	w, err := e.svc.OpenWallet(context.Background(), playerID, brl(e.t, initial))
	if err != nil {
		e.t.Fatal(err)
	}
	return w.ID(), playerID
}

type op struct {
	provider, ext, key, kind, amount, ref, round string
}

func cmd(t *testing.T, walletID, playerID uuid.UUID, o op) wager.Command {
	t.Helper()
	if o.provider == "" {
		o.provider = "provider-a"
	}
	if o.key == "" {
		o.key = o.provider + ":" + o.ext
	}
	if o.round == "" {
		o.round = "round-1"
	}
	c, err := wager.NewCommand(wager.CommandInput{
		ProviderID: o.provider, ExternalTransactionID: o.ext, IdempotencyKey: o.key,
		PlayerID: playerID.String(), WalletID: walletID.String(), RoundID: o.round, GameID: "fortune-chimp",
		Kind: o.kind, Amount: o.amount, Currency: "BRL", ReferenceExternalTransactionID: o.ref,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func httpSrc() wagering.Source {
	return wagering.Source{Channel: wagering.ChannelHTTP, CorrelationID: uuid.NewString()}
}

func sqsSrc(messageID string) wagering.Source {
	return wagering.Source{Channel: wagering.ChannelSQS, CorrelationID: messageID, CausationID: messageID,
		Inbox: &wagering.InboxMessage{Consumer: "test-consumer", MessageID: messageID}}
}

func (e *env) submit(c wager.Command, src wagering.Source) wagering.Result {
	e.t.Helper()
	res, err := e.svc.Submit(context.Background(), c, src)
	if err != nil {
		e.t.Fatalf("submit %s: %v", c, err)
	}
	return res
}

func (e *env) count(q string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.db.Owner.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// assertConsistent checks the stored balance against the ledger, the way the
// reconciliation endpoint does, and returns the stored balance.
func (e *env) assertConsistent(walletID uuid.UUID) string {
	e.t.Helper()
	rec, err := e.svc.Reconcile(context.Background(), walletID)
	if err != nil {
		e.t.Fatal(err)
	}
	if !rec.Consistent {
		e.t.Fatalf("wallet %s inconsistent: stored %s calculated %s", walletID, rec.StoredBalance, rec.CalculatedBalance)
	}
	return rec.StoredBalance.Amount()
}

func TestOpenWallet(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("1000.00")
	if n := e.count(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED'`, walletID); n != 1 {
		t.Fatalf("openings = %d", n)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, walletID); n != 2 {
		t.Fatalf("outbox events = %d, want Processed + BalanceChanged", n)
	}
	if _, err := e.svc.OpenWallet(context.Background(), playerID, brl(t, "5.00")); !errors.Is(err, wagering.ErrWalletExists) {
		t.Fatalf("second wallet for the same player: %v", err)
	}

	zeroID, _ := e.open("0.00")
	if n := e.count(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, zeroID); n != 0 {
		t.Fatalf("zero opening created %d transactions", n)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, zeroID); n != 0 {
		t.Fatalf("zero opening created %d events", n)
	}
	if got := e.assertConsistent(walletID); got != "1000.00" {
		t.Fatalf("balance = %s", got)
	}
}

// Required test 1: the same bet 50 times in parallel debits once.
func TestSameBetFiftyTimesInParallel(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	c := cmd(t, walletID, playerID, op{ext: "bet-1", kind: "BET", amount: "25.00"})

	var wg sync.WaitGroup
	results := make([]wagering.Result, 50)
	errs := make([]error, 50)
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			src := httpSrc()
			if i%2 == 1 { // half through the SQS path, each copy a redelivery with its own message id
				src = sqsSrc(fmt.Sprintf("msg-%d", i))
			}
			results[i], errs[i] = e.svc.Submit(context.Background(), c, src)
		}()
	}
	wg.Wait()

	fresh := 0
	var txID uuid.UUID
	for i := range 50 {
		if errs[i] != nil {
			t.Fatalf("copy %d: %v", i, errs[i])
		}
		r := results[i]
		if r.Transaction.Status != wager.Processed || r.Transaction.ResultBalance.Amount() != "75.00" {
			t.Fatalf("copy %d: %s balance %v", i, r.Transaction.Status, r.Transaction.ResultBalance)
		}
		if !r.Replay {
			fresh++
		}
		if txID == uuid.Nil {
			txID = r.Transaction.ID
		} else if r.Transaction.ID != txID {
			t.Fatal("copies returned different transaction ids")
		}
	}
	if fresh != 1 {
		t.Fatalf("%d copies were applied, want 1 (the other 49 must be replays)", fresh)
	}
	if n := e.count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if n := e.count(`SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL AND transaction_id = $1`, txID); n != 25 {
		t.Fatalf("completed inbox messages = %d, want 25", n)
	}
	if got := e.assertConsistent(walletID); got != "75.00" {
		t.Fatalf("balance = %s", got)
	}
}

// Required test 2: two distinct bets of 80.00 on 100.00.
func TestTwoBetsOfEightyOnOneHundred(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	cmds := []wager.Command{
		cmd(t, walletID, playerID, op{ext: "bet-a", kind: "BET", amount: "80.00"}),
		cmd(t, walletID, playerID, op{ext: "bet-b", kind: "BET", amount: "80.00"}),
	}
	// Two independent services with their own pools: two processes.
	svcs := []*wagering.Service{newService(e.db.App), newService(e.db.App)}
	var wg sync.WaitGroup
	results := make([]wagering.Result, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if results[i], err = svcs[i].Submit(context.Background(), cmds[i], httpSrc()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	statuses := map[wager.Status]int{}
	for _, r := range results {
		statuses[r.Transaction.Status]++
		if r.Transaction.Status == wager.Rejected && r.Transaction.FailureCode != failure.InsufficientFunds {
			t.Fatalf("rejection code = %s", r.Transaction.FailureCode)
		}
	}
	if statuses[wager.Processed] != 1 || statuses[wager.Rejected] != 1 {
		t.Fatalf("statuses = %v", statuses)
	}
	if n := e.count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if got := e.assertConsistent(walletID); got != "20.00" {
		t.Fatalf("balance = %s", got)
	}

	// Resending both changes nothing: the rejection is a persisted result.
	for i, c := range cmds {
		r := e.submit(c, httpSrc())
		if !r.Replay || r.Transaction.Status != results[i].Transaction.Status {
			t.Fatalf("resend %d: replay=%t status=%s", i, r.Replay, r.Transaction.Status)
		}
	}
	if got := e.assertConsistent(walletID); got != "20.00" {
		t.Fatalf("balance after resend = %s", got)
	}
}

// Required test 3: distinct wallets progress in parallel, while one wallet is
// held locked by another transaction.
func TestDistinctWalletsProgressInParallel(t *testing.T) {
	e := setup(t)
	hot, _ := e.open("100.00")

	// Hold the row lock of one wallet for the whole test.
	locker, err := e.db.App.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	if _, err := locker.Exec(context.Background(), `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, hot); err != nil {
		t.Fatal(err)
	}

	const wallets = 20
	type w struct{ id, player uuid.UUID }
	ws := make([]w, wallets)
	for i := range ws {
		ws[i].id, ws[i].player = e.open("10.00")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	start := time.Now()
	for i, x := range ws {
		for j := range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c := cmd(t, x.id, x.player, op{ext: fmt.Sprintf("p-%d-%d", i, j), kind: "BET", amount: "1.00"})
				if _, err := e.svc.Submit(ctx, c, httpSrc()); err != nil {
					t.Error(err)
				}
			}()
		}
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("independent wallets took %s while another wallet was locked", elapsed)
	}
	for _, x := range ws {
		if got := e.assertConsistent(x.id); got != "5.00" {
			t.Fatalf("wallet %s balance = %s", x.id, got)
		}
	}
}

func TestIdempotencyContract(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	bet := cmd(t, walletID, playerID, op{ext: "bet-1", kind: "BET", amount: "10.00"})
	first := e.submit(bet, httpSrc())
	if first.Replay {
		t.Fatal("first submission marked as replay")
	}

	// Other movements happen afterwards.
	e.submit(cmd(t, walletID, playerID, op{ext: "win-1", kind: "WIN", amount: "50.00"}), httpSrc())

	replay := e.submit(bet, httpSrc())
	if !replay.Replay || replay.Transaction.ID != first.Transaction.ID || replay.Transaction.ResultBalance.Amount() != "90.00" {
		t.Fatalf("replay must return the original balance 90.00: %+v", replay)
	}

	sameKeyOtherPayload := cmd(t, walletID, playerID, op{ext: "bet-1", kind: "BET", amount: "11.00"})
	if _, err := e.svc.Submit(context.Background(), sameKeyOtherPayload, httpSrc()); !errors.Is(err, wagering.ErrIdempotencyKeyReused) {
		t.Fatalf("same key, other payload: %v", err)
	}
	sameOperationOtherKey := cmd(t, walletID, playerID, op{ext: "bet-1", key: "another-key", kind: "BET", amount: "10.00"})
	if _, err := e.svc.Submit(context.Background(), sameOperationOtherKey, httpSrc()); !errors.Is(err, wagering.ErrDuplicateExternal) {
		t.Fatalf("same operation, other key: %v", err)
	}

	// Keys are scoped by provider: provider-b may reuse the same key and id.
	other := cmd(t, walletID, playerID, op{provider: "provider-b", ext: "bet-1", key: "provider-a:bet-1", kind: "BET", amount: "10.00"})
	if r := e.submit(other, httpSrc()); r.Replay || r.Transaction.Status != wager.Processed {
		t.Fatalf("provider-b: %+v", r)
	}

	missing := cmd(t, uuid.New(), playerID, op{ext: "bet-2", kind: "BET", amount: "1.00"})
	if _, err := e.svc.Submit(context.Background(), missing, httpSrc()); !failure.Is(err, failure.WalletNotFound) {
		t.Fatalf("missing wallet: %v", err)
	}
	if got := e.assertConsistent(walletID); got != "130.00" {
		t.Fatalf("balance = %s", got)
	}
}

func TestInboxAcrossChannels(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	bet := cmd(t, walletID, playerID, op{ext: "bet-1", kind: "BET", amount: "10.00"})

	viaHTTP := e.submit(bet, httpSrc())
	viaSQS := e.submit(bet, sqsSrc("msg-1"))
	if !viaSQS.Replay || viaSQS.Transaction.ID != viaHTTP.Transaction.ID {
		t.Fatalf("SQS copy of an HTTP operation: %+v", viaSQS)
	}
	// Redelivery of the same message: recognized by the inbox.
	again := e.submit(bet, sqsSrc("msg-1"))
	if !again.Replay || again.Transaction.ID != viaHTTP.Transaction.ID {
		t.Fatalf("redelivery: %+v", again)
	}
	// Same message id with another payload.
	other := cmd(t, walletID, playerID, op{ext: "bet-2", kind: "BET", amount: "10.00"})
	if _, err := e.svc.Submit(context.Background(), other, sqsSrc("msg-1")); !errors.Is(err, wagering.ErrMessageIDReused) {
		t.Fatalf("message id reuse: %v", err)
	}
	if n := e.count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
}

func TestLossAndRejectionsProduceNoLedger(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("10.00")
	loss := e.submit(cmd(t, walletID, playerID, op{ext: "loss-1", kind: "LOSS", amount: "0.00"}), httpSrc())
	if loss.Transaction.Status != wager.Processed {
		t.Fatal(loss.Transaction.Status)
	}
	rej := e.submit(cmd(t, walletID, playerID, op{ext: "bet-big", kind: "BET", amount: "10.01"}), httpSrc())
	if rej.Transaction.Status != wager.Rejected || rej.Transaction.FailureCode != failure.InsufficientFunds {
		t.Fatal(rej.Transaction.Status, rej.Transaction.FailureCode)
	}
	if n := e.count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID); n != 1 {
		t.Fatalf("entries = %d, want only the opening", n)
	}
	if n := e.count(`SELECT version FROM wallets WHERE id = $1`, walletID); n != 1 {
		t.Fatalf("version = %d, LOSS and rejections must not change it", n)
	}
	types := e.count(`SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type IN ('WagerTransactionProcessed','WagerTransactionRejected')`, walletID)
	if types != 3 { // opening, loss, rejection
		t.Fatalf("processed/rejected events = %d", types)
	}
}

// Required test 7: a reversal before its reference, then the reference.
func TestReversalBeforeReferenceIsResolvedByWorker(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	refund := cmd(t, walletID, playerID, op{ext: "refund-1", kind: "REFUND", amount: "30.00", ref: "bet-late"})
	pending := e.submit(refund, httpSrc())
	if pending.Transaction.Status != wager.PendingReference {
		t.Fatalf("status = %s", pending.Transaction.Status)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE event_type = 'WagerTransactionPendingReference'`); n != 1 {
		t.Fatalf("pending events = %d", n)
	}
	// Replay while pending returns the pending state.
	if r := e.submit(refund, httpSrc()); !r.Replay || r.Transaction.Status != wager.PendingReference {
		t.Fatalf("replay while pending: %+v", r)
	}

	e.submit(cmd(t, walletID, playerID, op{ext: "bet-late", kind: "BET", amount: "30.00"}), httpSrc())

	// A worker from another "instance" picks it up.
	worker := newService(e.db.App)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := worker.ResumePending(context.Background(), 10); err != nil {
			t.Fatal(err)
		}
		snap, err := e.svc.GetTransaction(context.Background(), pending.Transaction.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status == wager.Processed {
			if snap.ReferenceTransactionID == uuid.Nil || snap.ResultBalance.Amount() != "100.00" {
				t.Fatalf("resolved: %+v", snap)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still %s", snap.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := e.assertConsistent(walletID); got != "100.00" {
		t.Fatalf("balance = %s", got)
	}
	// A second refund of the same bet is rejected.
	again := e.submit(cmd(t, walletID, playerID, op{ext: "refund-2", kind: "REFUND", amount: "30.00", ref: "bet-late"}), httpSrc())
	if again.Transaction.FailureCode != failure.AlreadyReversed {
		t.Fatalf("second refund: %s %s", again.Transaction.Status, again.Transaction.FailureCode)
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	r := e.submit(cmd(t, walletID, playerID, op{ext: "rb-1", kind: "ROLLBACK", amount: "5.00", ref: "never"}), httpSrc())
	deadline := time.Now().Add(fastRetry.TTL + 3*time.Second)
	for {
		if _, err := e.svc.ResumePending(context.Background(), 10); err != nil {
			t.Fatal(err)
		}
		snap, _ := e.svc.GetTransaction(context.Background(), r.Transaction.ID)
		if snap.Status == wager.Rejected {
			if snap.FailureCode != failure.ReferenceNotFound || snap.Attempts == 0 {
				t.Fatalf("expired: %+v", snap)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still %s after TTL", snap.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE event_type = 'WagerTransactionRejected' AND payload->'data'->>'failureCode' = 'REFERENCE_NOT_FOUND'`); n != 1 {
		t.Fatalf("rejection events = %d", n)
	}
}

func TestRefundRollbackCombinations(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	s := func(o op) wagering.Result { return e.submit(cmd(t, walletID, playerID, o), httpSrc()) }

	s(op{ext: "bet-1", kind: "BET", amount: "40.00"})
	if r := s(op{ext: "refund-1", kind: "REFUND", amount: "40.00", ref: "bet-1"}); r.Transaction.Status != wager.Processed {
		t.Fatal(r.Transaction.FailureCode)
	}
	if r := s(op{ext: "rollback-bet", kind: "ROLLBACK", amount: "40.00", ref: "bet-1"}); r.Transaction.FailureCode != failure.AlreadyReversed {
		t.Fatalf("rollback of a refunded bet: %s", r.Transaction.FailureCode)
	}
	// ROLLBACK of the REFUND debits again: the bet stands.
	if r := s(op{ext: "rollback-refund", kind: "ROLLBACK", amount: "40.00", ref: "refund-1"}); r.Transaction.Status != wager.Processed {
		t.Fatal(r.Transaction.FailureCode)
	}
	if r := s(op{ext: "rollback-rollback", kind: "ROLLBACK", amount: "40.00", ref: "rollback-refund"}); r.Transaction.FailureCode != failure.InvalidReferenceKind {
		t.Fatalf("rollback of rollback: %s", r.Transaction.FailureCode)
	}
	if r := s(op{ext: "refund-other-round", kind: "REFUND", amount: "40.00", ref: "bet-1", round: "round-2"}); r.Transaction.FailureCode != failure.ReferenceMismatch {
		t.Fatalf("refund in another round: %s", r.Transaction.FailureCode)
	}
	if got := e.assertConsistent(walletID); got != "60.00" {
		t.Fatalf("balance = %s", got)
	}

	// Rollback of a win that the balance no longer covers.
	s(op{ext: "win-1", kind: "WIN", amount: "50.00"})
	s(op{ext: "bet-all", kind: "BET", amount: "110.00"})
	r := s(op{ext: "rollback-win", kind: "ROLLBACK", amount: "50.00", ref: "win-1"})
	if r.Transaction.FailureCode != failure.ReversalInsufficientFunds {
		t.Fatalf("rollback without funds: %s %s", r.Transaction.Status, r.Transaction.FailureCode)
	}
	if got := e.assertConsistent(walletID); got != "0.00" {
		t.Fatalf("balance = %s", got)
	}
}

func TestProviderCannotReferenceAnotherProvider(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	e.submit(cmd(t, walletID, playerID, op{provider: "provider-a", ext: "bet-1", kind: "BET", amount: "10.00"}), httpSrc())
	r := e.submit(cmd(t, walletID, playerID, op{provider: "provider-b", ext: "refund-1", kind: "REFUND", amount: "10.00", ref: "bet-1"}), httpSrc())
	if r.Transaction.Status != wager.PendingReference {
		t.Fatalf("provider-b must not see provider-a's bet: %s", r.Transaction.Status)
	}
}

// Regression (senior review C1): a wallet opened with 0.00 has no ledger
// entry; its first credit starts the chain at version 2.
func TestWalletOpenedWithZeroReceivesCredits(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("0.00")
	win := e.submit(cmd(t, walletID, playerID, op{ext: "win-1", kind: "WIN", amount: "1.00"}), httpSrc())
	if win.Transaction.Status != wager.Processed {
		t.Fatalf("first credit: %s %s", win.Transaction.Status, win.Transaction.FailureCode)
	}
	e.submit(cmd(t, walletID, playerID, op{ext: "bet-1", kind: "BET", amount: "0.40"}), httpSrc())
	if got := e.assertConsistent(walletID); got != "0.60" {
		t.Fatalf("balance = %s", got)
	}
}

// Regression (senior review A1): a currency mismatch is a persisted business
// rejection, not a transient failure.
func TestCurrencyMismatchIsAPersistedRejection(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	c, err := wager.NewCommand(wager.CommandInput{
		ProviderID: "provider-a", ExternalTransactionID: "usd-1", IdempotencyKey: "provider-a:usd-1",
		PlayerID: playerID.String(), WalletID: walletID.String(), RoundID: "round-1", GameID: "g",
		Kind: "BET", Amount: "1.00", Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := e.submit(c, httpSrc())
	if r.Transaction.Status != wager.Rejected || r.Transaction.FailureCode != failure.CurrencyMismatch {
		t.Fatalf("currency mismatch: %s %s", r.Transaction.Status, r.Transaction.FailureCode)
	}
	if again := e.submit(c, httpSrc()); !again.Replay || again.Transaction.ID != r.Transaction.ID {
		t.Fatalf("replay: %+v", again)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE event_type = 'WagerTransactionRejected' AND aggregate_id = $1`, walletID); n != 1 {
		t.Fatalf("rejection events = %d", n)
	}
	if got := e.assertConsistent(walletID); got != "100.00" {
		t.Fatalf("balance = %s", got)
	}
}

// Regression (senior review M1): an operation for another player's wallet
// must not reveal that wallet's balance.
func TestPlayerMismatchDoesNotRevealTheBalance(t *testing.T) {
	e := setup(t)
	walletID, _ := e.open("100.00")
	r := e.submit(cmd(t, walletID, uuid.New(), op{ext: "probe", kind: "BET", amount: "1.00"}), httpSrc())
	if r.Transaction.FailureCode != failure.PlayerWalletMismatch || r.Transaction.ResultBalance != nil {
		t.Fatalf("player mismatch: %s balance %v", r.Transaction.FailureCode, r.Transaction.ResultBalance)
	}
}

// Regression (senior review M2): a credit that would overflow the balance is
// a persisted rejection, not an internal error.
func TestCreditOverflowIsRejected(t *testing.T) {
	e := setup(t)
	walletID, playerID := e.open("100.00")
	r := e.submit(cmd(t, walletID, playerID, op{ext: "huge", kind: "WIN", amount: "92233720368547758.07"}), httpSrc())
	if r.Transaction.Status != wager.Rejected || r.Transaction.FailureCode != failure.BalanceLimitExceeded {
		t.Fatalf("overflow: %s %s", r.Transaction.Status, r.Transaction.FailureCode)
	}
	if got := e.assertConsistent(walletID); got != "100.00" {
		t.Fatalf("balance = %s", got)
	}
}
