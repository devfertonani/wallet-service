package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/fakewallet"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
)

// TestSmokeAgainstFakeService drives seed, run and report end to end over
// real HTTP against three "instances" of an in-memory fake (with 3% of 503
// answers to exercise the retry path), then checks the journal oracle against
// the fake's state. The SQL part of verify needs the real stack.
func TestSmokeAgainstFakeService(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end run takes ~10 s")
	}
	fake := fakewallet.New()
	fake.Fail503 = 0.03
	var targets []string
	for i := 0; i < 3; i++ {
		srv := httptest.NewServer(fake.Handler())
		t.Cleanup(srv.Close)
		targets = append(targets, srv.URL)
	}
	dir := t.TempDir()
	common := []string{
		"-out", dir, "-targets", strings.Join(targets, ","), "-token-url", targets[0] + "/token",
		"-sqs=false", "-db-url=",
	}
	ctx := context.Background()

	if err := cmdSeed(ctx, append([]string{"-scenario", "smoke", "-wallets", "20"}, common...)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := cmdRun(ctx, append([]string{"-scenario", "smoke", "-rate", "60", "-duration", "6s", "-warmup", "1s",
		"-max-delay", "200ms", "-metrics-addr", ""}, common...)); err != nil {
		t.Fatalf("run: %v", err)
	}

	wallets, err := journal.ReadWallets(filepath.Join(dir, result.WalletsFile))
	if err != nil || len(wallets) != 20 {
		t.Fatalf("wallets: %d %v", len(wallets), err)
	}
	ops, err := journal.ReadOps(filepath.Join(dir, result.JournalFile))
	if err != nil {
		t.Fatal(err)
	}
	tasks := map[string]int{}
	for _, op := range ops {
		tasks[op.Task]++
	}
	for _, want := range []string{"round-http", "duplicate", "reversal-first"} {
		if tasks[want] == 0 {
			t.Errorf("no %s operation in the journal: %v", want, tasks)
		}
	}

	txs, dbw := fake.State()
	cross := journal.CrossCheck(wallets, ops, txs, dbw)
	if len(cross.Violations) != 0 {
		t.Fatalf("oracle violations against the fake: %+v", cross.Violations)
	}
	if cross.Stats.Replays == 0 || cross.Stats.Probes == 0 {
		t.Errorf("duplicates did not produce replays/probes: %+v", cross.Stats)
	}

	var run result.Run
	if err := result.Read(dir, result.RunFile, &run); err != nil {
		t.Fatal(err)
	}
	if !run.Valid || run.HTTPRetries == 0 || run.Resolution.Count() == 0 {
		t.Fatalf("run: valid=%v reasons=%v retries=%d resolutions=%d", run.Valid, run.InvalidReasons, run.HTTPRetries, run.Resolution.Count())
	}
	if run.TokenFetches["provider-a"] != 1 || run.TokenFetches["provider-b"] != 1 {
		t.Errorf("tokens must be fetched once per client, got %v", run.TokenFetches)
	}

	// Stand-in for verify (which needs Postgres): the oracle result only.
	if err := result.Write(dir, result.VerifyFile, result.Verify{Pass: true, Cross: cross, SQSEndToEnd: cross.SQSEndToEnd}); err != nil {
		t.Fatal(err)
	}
	if err := cmdReport([]string{"-out", dir}); err != nil {
		t.Fatalf("report: %v", err)
	}
	md, err := os.ReadFile(filepath.Join(dir, result.ReportFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Result: **PASS**", "| http | BET |", "p99", "idempotentReplay"} {
		if !strings.Contains(string(md), want) {
			t.Errorf("report.md misses %q", want)
		}
	}
	if testing.Verbose() {
		t.Logf("report.md:\n%s", md)
	}
	if _, err := os.Stat(filepath.Join(dir, result.SummaryFile)); err != nil {
		t.Error(err)
	}
}
