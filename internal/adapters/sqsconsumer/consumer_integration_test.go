//go:build integration

package sqsconsumer_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/devfertonani/wallet-service/internal/adapters/pgstore"
	"github.com/devfertonani/wallet-service/internal/adapters/sqsconsumer"
	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
	"github.com/devfertonani/wallet-service/internal/testsupport/awstest"
	"github.com/devfertonani/wallet-service/internal/testsupport/pgtest"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fixture struct {
	aws      *awstest.Env
	db       *pgtest.DB
	uow      wagering.UnitOfWork
	metrics  *metrics.Metrics
	walletID uuid.UUID
	playerID uuid.UUID
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{aws: awstest.New(t), db: pgtest.New(t), metrics: metrics.New(), playerID: uuid.New()}
	f.uow = pgstore.NewUnitOfWork(f.db.App)
	initial, _ := money.Parse("100.00", "BRL")
	w, err := f.service(f.uow).OpenWallet(context.Background(), f.playerID, initial)
	if err != nil {
		t.Fatal(err)
	}
	f.walletID = w.ID()
	return f
}

func (f *fixture) service(uow wagering.UnitOfWork) *wagering.Service {
	return wagering.NewService(uow, f.metrics, discard, wager.RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, TTL: time.Minute})
}

func (f *fixture) consumer(svc *wagering.Service, client sqsconsumer.SQS) *sqsconsumer.Consumer {
	q := f.aws.Queues
	return sqsconsumer.New(svc, client, func() awsclient.Queues { return q },
		sqsconsumer.Options{WaitTime: time.Second, MessageTimeout: 10 * time.Second}, f.metrics, discard)
}

// run starts the consumer loop and returns a stop function that waits for
// the loop to return, the way the Fx OnStop hook does.
func run(c *sqsconsumer.Consumer) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Loop(ctx); close(done) }()
	return func() { cancel(); <-done }
}

func (f *fixture) message(messageID, ext, kind, amount string) map[string]any {
	return map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339),
		"data": map[string]any{
			"providerId": "provider-a", "externalTransactionId": ext, "idempotencyKey": "provider-a:" + ext,
			"playerId": f.playerID.String(), "walletId": f.walletID.String(), "roundId": "round-1", "gameId": "fortune-chimp",
			"kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"},
		},
	}
}

func (f *fixture) count(t *testing.T, q string, args ...any) int {
	t.Helper()
	var n int
	if err := f.db.Owner.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *fixture) debits(t *testing.T) int {
	return f.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, f.walletID)
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func handled(m *metrics.Metrics, outcome string) int {
	return int(testutil.ToFloat64(m.SQSMessages.WithLabelValues(outcome)))
}

func TestRedeliveryIsDeduplicatedByTheInbox(t *testing.T) {
	f := setup(t)
	stop := run(f.consumer(f.service(f.uow), f.aws.SQS))
	defer stop()

	msg := f.message("msg-1", "bet-1", "BET", "25.00")
	f.aws.Send(t, msg, f.walletID.String(), "dedup-1")
	eventually(t, 10*time.Second, "first delivery", func() bool { return handled(f.metrics, "done") == 1 })
	// Same message, new SQS deduplication id: the FIFO window does not hide
	// it, so only the inbox can recognize it.
	f.aws.Send(t, msg, f.walletID.String(), "dedup-2")
	eventually(t, 10*time.Second, "redelivery", func() bool { return handled(f.metrics, "done") == 2 })

	if n := f.debits(t); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if n := f.count(t, `SELECT count(*) FROM inbox_messages WHERE message_id = 'msg-1' AND completed_at IS NOT NULL`); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}
	eventually(t, 5*time.Second, "empty queue", func() bool { return f.aws.Depth(t, f.aws.Queues.WagerURL) == 0 })
}

// crashingDelete simulates a consumer that dies after the commit and before
// deleting the message: the delete never reaches SQS.
type crashingDelete struct {
	*sqs.Client
	mu       sync.Mutex
	receipts []string
}

func (c *crashingDelete) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.receipts = append(c.receipts, aws.ToString(in.ReceiptHandle))
	return nil, errors.New("process killed before DeleteMessage")
}

// Required test 5: interrupt a consumer after the commit and before the
// message is removed; the redelivery is recognized and nothing is applied twice.
func TestCrashAfterCommitBeforeDelete(t *testing.T) {
	f := setup(t)
	crashing := &crashingDelete{Client: f.aws.SQS}
	stop := run(f.consumer(f.service(f.uow), crashing))
	f.aws.Send(t, f.message("msg-crash", "bet-crash", "BET", "30.00"), f.walletID.String(), "d-crash")
	eventually(t, 10*time.Second, "commit by the first consumer", func() bool {
		return f.count(t, `SELECT count(*) FROM inbox_messages WHERE message_id = 'msg-crash' AND completed_at IS NOT NULL`) == 1
	})
	stop()
	if f.debits(t) != 1 {
		t.Fatal("first consumer did not commit")
	}
	// The visibility timeout (30 s) expires: emulate it, then a second
	// instance receives the message again.
	crashing.mu.Lock()
	receipt := crashing.receipts[0]
	crashing.mu.Unlock()
	if _, err := f.aws.SQS.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(f.aws.Queues.WagerURL), ReceiptHandle: aws.String(receipt), VisibilityTimeout: 0,
	}); err != nil {
		t.Fatal(err)
	}
	stop2 := run(f.consumer(f.service(f.uow), f.aws.SQS))
	defer stop2()
	eventually(t, 10*time.Second, "redelivery handled", func() bool { return handled(f.metrics, "done") == 2 })
	eventually(t, 5*time.Second, "message removed", func() bool { return f.aws.Depth(t, f.aws.Queues.WagerURL) == 0 })
	if n := f.debits(t); n != 1 {
		t.Fatalf("debits = %d after redelivery", n)
	}
}

func TestPermanentFailuresGoToTheDLQ(t *testing.T) {
	f := setup(t)
	stop := run(f.consumer(f.service(f.uow), f.aws.SQS))
	defer stop()

	numeric := f.message("msg-num", "num", "BET", "1.00")
	numeric["data"].(map[string]any)["money"] = map[string]any{"amount": 25.00, "currency": "BRL"}
	opening := f.message("msg-open", "open", "OPENING", "1.00")
	wrongType := f.message("msg-type", "type", "BET", "1.00")
	wrongType["type"] = "SomethingElse"

	f.aws.Send(t, "not json", "g1", "d1")
	f.aws.Send(t, numeric, "g2", "d2")
	f.aws.Send(t, opening, "g3", "d3")
	f.aws.Send(t, wrongType, "g4", "d4")

	eventually(t, 15*time.Second, "4 messages in the DLQ", func() bool { return f.aws.Depth(t, f.aws.Queues.WagerDLQURL) == 4 })
	reasons := map[string]bool{}
	for _, m := range f.aws.Drain(t, f.aws.Queues.WagerDLQURL, 3*time.Second) {
		reasons[aws.ToString(m.MessageAttributes["reason"].StringValue)] = true
	}
	for _, want := range []string{"INVALID_MESSAGE", "UNSUPPORTED_KIND", "UNSUPPORTED_MESSAGE_TYPE"} {
		if !reasons[want] {
			t.Errorf("missing DLQ reason %s in %v", want, reasons)
		}
	}
	if f.aws.Depth(t, f.aws.Queues.WagerURL) != 0 {
		t.Fatal("permanent failures left in the main queue")
	}
	if n := f.count(t, `SELECT count(*) FROM inbox_messages`); n != 0 {
		t.Fatalf("inbox rows = %d for rejected messages", n)
	}
	if got := testutil.ToFloat64(f.metrics.SQSDLQ.WithLabelValues("INVALID_MESSAGE")); got != 2 {
		t.Fatalf("dlq metric INVALID_MESSAGE = %v", got)
	}
}

// flaky fails the first n units of work as if the database were unavailable.
type flaky struct {
	wagering.UnitOfWork
	left atomic.Int32
}

func (f *flaky) Run(ctx context.Context, fn func(context.Context, wagering.Repos) error) error {
	if f.left.Add(-1) >= 0 {
		return fmt.Errorf("%w: connection refused (injected)", wagering.ErrTransient)
	}
	return f.UnitOfWork.Run(ctx, fn)
}

func TestTransientFailureIsRetriedWithBackoff(t *testing.T) {
	f := setup(t)
	uow := &flaky{UnitOfWork: f.uow}
	uow.left.Store(1)
	stop := run(f.consumer(f.service(uow), f.aws.SQS))
	defer stop()

	start := time.Now()
	f.aws.Send(t, f.message("msg-retry", "bet-retry", "BET", "10.00"), f.walletID.String(), "d-retry")
	eventually(t, 20*time.Second, "processed after retry", func() bool { return handled(f.metrics, "done") == 1 })
	if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
		t.Fatalf("redelivered after %s, before the backoff", elapsed)
	}
	if got := testutil.ToFloat64(f.metrics.SQSRetries); got != 1 {
		t.Fatalf("retries = %v", got)
	}
	if n := f.debits(t); n != 1 {
		t.Fatalf("debits = %d", n)
	}
}

// cancelOnFirst cancels the consumer (SIGTERM) while the first message is
// being processed.
type cancelOnFirst struct {
	wagering.UnitOfWork
	once   sync.Once
	cancel func()
}

func (c *cancelOnFirst) Run(ctx context.Context, fn func(context.Context, wagering.Repos) error) error {
	c.once.Do(c.cancel)
	return c.UnitOfWork.Run(ctx, fn)
}

func TestShutdownFinishesInFlightAndReleasesTheRest(t *testing.T) {
	f := setup(t)
	for i := range 3 {
		f.aws.Send(t, f.message(fmt.Sprintf("msg-%d", i), fmt.Sprintf("bet-%d", i), "BET", "1.00"), f.walletID.String(), fmt.Sprintf("d-%d", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	uow := &cancelOnFirst{UnitOfWork: f.uow, cancel: cancel}
	c := f.consumer(f.service(uow), f.aws.SQS)
	done := make(chan struct{})
	go func() { c.Loop(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop")
	}
	// The in-flight message was finished, not abandoned.
	if n := f.debits(t); n < 1 {
		t.Fatalf("debits after shutdown = %d, the in-flight message must complete", n)
	}

	// The released messages are visible at once, not after the 30 s timeout.
	start := time.Now()
	stop := run(f.consumer(f.service(f.uow), f.aws.SQS))
	defer stop()
	eventually(t, 8*time.Second, "released messages processed", func() bool { return f.debits(t) == 3 })
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("released messages took %s", elapsed)
	}
}

// The same operation arrives at the same time through HTTP and SQS.
func TestHTTPAndSQSRaceForTheSameOperation(t *testing.T) {
	f := setup(t)
	svc := f.service(f.uow)
	stop := run(f.consumer(svc, f.aws.SQS))
	defer stop()

	cmd, err := wager.NewCommand(wager.CommandInput{
		ProviderID: "provider-a", ExternalTransactionID: "bet-race", IdempotencyKey: "provider-a:bet-race",
		PlayerID: f.playerID.String(), WalletID: f.walletID.String(), RoundID: "round-1", GameID: "fortune-chimp",
		Kind: "BET", Amount: "40.00", Currency: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 10 {
		m := f.message(fmt.Sprintf("msg-race-%d", i), "bet-race", "BET", "40.00")
		f.aws.Send(t, m, f.walletID.String(), fmt.Sprintf("d-race-%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Submit(context.Background(), cmd, wagering.Source{Channel: wagering.ChannelHTTP, CorrelationID: "http"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	eventually(t, 15*time.Second, "10 SQS copies handled", func() bool { return handled(f.metrics, "done") == 10 })
	if n := f.debits(t); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if n := f.count(t, `SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL`); n != 10 {
		t.Fatalf("completed inbox rows = %d", n)
	}
}
