package journal

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func i64(v int64) *int64 { return &v }

func TestEffect(t *testing.T) {
	cases := []struct {
		kind, ref string
		want      int64
	}{
		{"BET", "", -500}, {"WIN", "", 500}, {"LOSS", "", 0}, {"REFUND", "BET", 500},
		{"ROLLBACK", "BET", 500}, {"ROLLBACK", "WIN", -500}, {"ROLLBACK", "REFUND", -500},
	}
	for _, c := range cases {
		if got := Effect(c.kind, c.ref, 500); got != c.want {
			t.Errorf("Effect(%s, %s) = %d, want %d", c.kind, c.ref, got, c.want)
		}
	}
	if Moves("LOSS", "", 0) {
		t.Error("LOSS must not move")
	}
}

type fixture struct {
	wallets []Wallet
	ops     []Op
	txs     []DBTx
}

func (f *fixture) http(ext, kind string, amount int64, status int, st, code, tx string, bal int64) Op {
	op := Op{Channel: ChannelHTTP, Provider: "provider-a", ExtID: ext, Kind: kind, Amount: amount, WalletID: "w1",
		HTTPStatus: status, Status: st, FailureCode: code, TransactionID: tx, BalanceMinor: i64(bal), Scheduled: time.Unix(100, 0)}
	f.ops = append(f.ops, op)
	return op
}

func (f *fixture) persist(ext, kind string, amount int64, status, code, id string) {
	f.txs = append(f.txs, DBTx{ID: id, Provider: "provider-a", ExtID: ext, Kind: kind, Status: status, FailureCode: code,
		Amount: amount, WalletID: "w1", CreatedAt: time.Unix(101, 0)})
}

func newFixture() *fixture {
	return &fixture{wallets: []Wallet{{ID: "w1", OpeningMinor: 10000, Currency: "BRL"}}}
}

func checks(r CrossResult) string {
	var s []string
	for _, v := range r.Violations {
		s = append(s, v.Check)
	}
	return strings.Join(s, ",")
}

func TestCrossCheckHappyPathWithReplaysRejectionsAndReversal(t *testing.T) {
	f := newFixture()
	// BET 30.00 processed, answered twice (the copy is a replay).
	f.http("e1", "BET", 3000, 200, "PROCESSED", "", "t1", 7000)
	cp := f.http("e1", "BET", 3000, 200, "PROCESSED", "", "t1", 7000)
	f.ops[len(f.ops)-1].Role, f.ops[len(f.ops)-1].Replay = RoleCopy, true
	_ = cp
	f.persist("e1", "BET", 3000, "PROCESSED", "", "t1")
	// LOSS: processed, no movement.
	f.http("e2", "LOSS", 0, 200, "PROCESSED", "", "t2", 7000)
	f.persist("e2", "LOSS", 0, "PROCESSED", "", "t2")
	// BET rejected for funds: no movement.
	f.http("e3", "BET", 900000, 422, "REJECTED", "INSUFFICIENT_FUNDS", "t3", 7000)
	f.persist("e3", "BET", 900000, "REJECTED", "INSUFFICIENT_FUNDS", "t3")
	// ROLLBACK of the BET, sent by SQS: credit.
	f.ops = append(f.ops, Op{Channel: ChannelSQS, Provider: "provider-a", ExtID: "e4", Kind: "ROLLBACK", RefKind: "BET",
		Amount: 3000, WalletID: "w1", Status: StatusSent, Scheduled: time.Unix(100, 0)})
	f.persist("e4", "ROLLBACK", 3000, "PROCESSED", "", "t4")
	// A request that timed out and was never applied.
	f.ops = append(f.ops, Op{Channel: ChannelHTTP, Provider: "provider-a", ExtID: "e5", Kind: "WIN", Amount: 100, WalletID: "w1", Status: StatusError})
	// Conflict probe answered as expected.
	f.ops = append(f.ops, Op{Channel: ChannelHTTP, Role: RoleProbe, Expect: "409:IDEMPOTENCY_KEY_REUSED", Provider: "provider-a",
		ExtID: "e1", Kind: "BET", Amount: 3001, WalletID: "w1", HTTPStatus: 409, ErrorCode: "IDEMPOTENCY_KEY_REUSED"})

	r := CrossCheck(f.wallets, f.ops, f.txs, []DBWallet{{ID: "w1", Balance: 10000, Version: 3}})
	if len(r.Violations) != 0 {
		t.Fatalf("unexpected violations: %+v", r.Violations)
	}
	if r.Stats.LogicalOps != 5 || r.Stats.Copies != 1 || r.Stats.Replays != 1 || r.Stats.Probes != 1 || r.Stats.UnknownNotApplied != 1 {
		t.Fatalf("stats: %+v", r.Stats)
	}
	if r.SQSEndToEnd.Count() != 1 || r.SQSEndToEnd.Max() != time.Second {
		t.Fatalf("sqs e2e: %d %v", r.SQSEndToEnd.Count(), r.SQSEndToEnd.Max())
	}
}

func TestCrossCheckDetectsLostAndDoubleUpdates(t *testing.T) {
	f := newFixture()
	f.http("e1", "BET", 3000, 200, "PROCESSED", "", "t1", 7000)
	f.persist("e1", "BET", 3000, "PROCESSED", "", "t1")
	// Stored balance says the BET was applied twice: lost/extra update.
	r := CrossCheck(f.wallets, f.ops, f.txs, []DBWallet{{ID: "w1", Balance: 4000, Version: 3}})
	if got := checks(r); got != "balance_vs_journal,version_vs_journal" {
		t.Fatalf("violations = %s", got)
	}
}

func TestCrossCheckDetectsMismatches(t *testing.T) {
	f := newFixture()
	f.http("e1", "BET", 3000, 200, "PROCESSED", "", "t1", 7000)
	f.persist("e1", "BET", 3000, "REJECTED", "INSUFFICIENT_FUNDS", "t9") // status and id differ
	f.http("e2", "BET", 100, 200, "PROCESSED", "", "t2", 6900)
	c := f.http("e2", "BET", 100, 200, "PROCESSED", "", "t2b", 6800) // copy with another result
	_ = c
	f.ops[len(f.ops)-1].Role = RoleCopy
	f.persist("e2", "BET", 100, "PROCESSED", "", "t2")
	f.ops = append(f.ops, Op{Channel: ChannelSQS, Provider: "provider-a", ExtID: "e3", Kind: "WIN", Amount: 5, WalletID: "w1", Status: StatusSent})
	f.persist("e9", "WIN", 5, "PROCESSED", "", "t99") // not in journal
	f.ops = append(f.ops, Op{Channel: ChannelHTTP, Role: RoleProbe, Expect: "409:DUPLICATE_EXTERNAL_TRANSACTION",
		Provider: "provider-a", ExtID: "e2", HTTPStatus: 200, Status: "PROCESSED"})

	r := CrossCheck(f.wallets, f.ops, f.txs, []DBWallet{{ID: "w1", Balance: 9900, Version: 2}})
	got := checks(r)
	for _, want := range []string{"probe_unexpected", "transaction_id_mismatch", "status_mismatch", "replay_mismatch",
		"double_original", "sqs_message_not_applied", "unexpected_transaction"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
}

func TestCrossCheckPayloadAndMissingWallet(t *testing.T) {
	f := newFixture()
	f.wallets = append(f.wallets, Wallet{ID: "w2", OpeningMinor: 100})
	f.http("e1", "BET", 3000, 200, "PROCESSED", "", "t1", 7000)
	f.persist("e1", "BET", 3001, "PROCESSED", "", "t1")
	r := CrossCheck(f.wallets, f.ops, f.txs, []DBWallet{{ID: "w1", Balance: 6999, Version: 2}})
	got := checks(r)
	if !strings.Contains(got, "payload_mismatch") || !strings.Contains(got, "wallet_missing") {
		t.Fatalf("violations = %s", got)
	}
}

func TestJournalRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(filepath.Join(dir, "j.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := w.Write(Op{Seq: int64(i), Kind: "BET", BalanceMinor: i64(int64(i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	ops, err := ReadOps(filepath.Join(dir, "j.ndjson"))
	if err != nil || len(ops) != 3 || *ops[2].BalanceMinor != 2 {
		t.Fatalf("read back %d ops, err %v", len(ops), err)
	}
	ws := []Wallet{{ID: "a", OpeningMinor: 1}}
	if err := WriteWallets(filepath.Join(dir, "w.json"), ws); err != nil {
		t.Fatal(err)
	}
	back, err := ReadWallets(filepath.Join(dir, "w.json"))
	if err != nil || len(back) != 1 || back[0].ID != "a" {
		t.Fatalf("wallets: %+v %v", back, err)
	}
}
