//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/devfertonani/wallet-service/internal/adapters/outbox"
	"github.com/devfertonani/wallet-service/internal/adapters/pgstore"
	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
	"github.com/devfertonani/wallet-service/internal/testsupport/awstest"
	"github.com/devfertonani/wallet-service/internal/testsupport/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fixture struct {
	aws *awstest.Env
	db  *pgtest.DB
}

func setup(t *testing.T) *fixture {
	t.Helper()
	return &fixture{aws: awstest.New(t), db: pgtest.New(t)}
}

// openWallets produces outbox events the way the application does: each
// opening writes WagerTransactionProcessed and WalletBalanceChanged.
func (f *fixture) openWallets(t *testing.T, n int) {
	t.Helper()
	svc := wagering.NewService(pgstore.NewUnitOfWork(f.db.App), metrics.New(), discard, wager.RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Second, TTL: time.Minute})
	initial, _ := money.Parse("10.00", "BRL")
	for range n {
		if _, err := svc.OpenWallet(context.Background(), uuid.New(), initial); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *fixture) publisher(owner string, client outbox.SNSPublisher, lease time.Duration) *outbox.Publisher {
	topic := f.aws.Queues.EventsTopic
	return outbox.NewPublisher(f.db.App, client, func() string { return topic },
		outbox.Options{Owner: owner, BatchSize: 25, Lease: lease, Poll: 20 * time.Millisecond}, metrics.New(), discard)
}

func (f *fixture) count(t *testing.T, q string) int {
	t.Helper()
	var n int
	if err := f.db.Owner.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
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

func runLoop(p *outbox.Publisher) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Loop(ctx); close(done) }()
	return func() { cancel(); <-done }
}

// receivedEventIDs reads the subscription queue and counts deliveries per eventId.
func (f *fixture) receivedEventIDs(t *testing.T, want int) map[string]int {
	t.Helper()
	got := map[string]int{}
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < want && time.Now().Before(deadline) {
		for _, m := range f.aws.Drain(t, f.aws.EventsQueueURL, time.Second) {
			var env struct {
				EventID string `json:"eventId"`
			}
			if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &env); err != nil {
				t.Fatalf("event body: %v", err)
			}
			got[env.EventID]++
		}
	}
	return got
}

// Required test 6: two publishers compete for the same outbox.
func TestConcurrentPublishers(t *testing.T) {
	f := setup(t)
	f.openWallets(t, 60) // 120 events
	// Two independent pools: two processes.
	other, err := pgxpool.New(context.Background(), f.db.AppURL)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	topic := f.aws.Queues.EventsTopic
	p1 := f.publisher("instance-1", f.aws.SNS, 30*time.Second)
	p2 := outbox.NewPublisher(other, f.aws.SNS, func() string { return topic },
		outbox.Options{Owner: "instance-2", BatchSize: 25, Lease: 30 * time.Second, Poll: 20 * time.Millisecond}, metrics.New(), discard)
	stop1, stop2 := runLoop(p1), runLoop(p2)
	eventually(t, 30*time.Second, "outbox drained", func() bool {
		return f.count(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	stop1()
	stop2()

	if n := f.count(t, `SELECT count(*) FROM outbox_events WHERE attempts <> 1`); n != 0 {
		t.Fatalf("%d events were claimed more than once without any failure", n)
	}
	if n := f.count(t, `SELECT count(DISTINCT locked_by) FROM outbox_events`); n != 0 {
		t.Fatalf("leases left behind: %d", n)
	}
	got := f.receivedEventIDs(t, 120)
	if len(got) != 120 {
		t.Fatalf("received %d distinct events, want 120", len(got))
	}
	for id, n := range got {
		if n != 1 {
			t.Errorf("event %s delivered %d times", id, n)
		}
	}
}

// Interruption between claim and publication: the claimant dies holding the
// lease; another instance takes over when it expires.
func TestAbandonedClaimIsTakenOverAfterTheLease(t *testing.T) {
	f := setup(t)
	f.openWallets(t, 5)
	dead := f.publisher("dead-instance", f.aws.SNS, 500*time.Millisecond)
	claimed, err := dead.Claim(context.Background())
	if err != nil || len(claimed) != 10 {
		t.Fatalf("claim: %d, %v", len(claimed), err)
	}
	// Nothing is published by the dead instance. Before the lease expires,
	// nobody else can take the events.
	alive := f.publisher("alive-instance", f.aws.SNS, 30*time.Second)
	if n, err := alive.RunOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("claimed %d leased events, err %v", n, err)
	}
	time.Sleep(600 * time.Millisecond)
	if n, err := alive.RunOnce(context.Background()); err != nil || n != 10 {
		t.Fatalf("after the lease: claimed %d, err %v", n, err)
	}
	if n := f.count(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("unpublished = %d", n)
	}
	if got := f.receivedEventIDs(t, 10); len(got) != 10 {
		t.Fatalf("received %d", len(got))
	}
}

// lostAck publishes for real and then fails, as if the process died between
// the publication and the outbox update.
type lostAck struct {
	*sns.Client
	fail atomic.Bool
}

func (l *lostAck) PublishBatch(ctx context.Context, in *sns.PublishBatchInput, opts ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	out, err := l.Client.PublishBatch(ctx, in, opts...)
	if err == nil && l.fail.Load() {
		return nil, errors.New("connection reset after publish (injected)")
	}
	return out, err
}

// Interruption between publication and confirmation: the event is published
// again later with the same eventId.
func TestRepublicationKeepsTheEventID(t *testing.T) {
	f := setup(t)
	f.openWallets(t, 1)
	client := &lostAck{Client: f.aws.SNS}
	client.fail.Store(true)
	p := f.publisher("instance-1", client, 30*time.Second)
	if n, err := p.RunOnce(context.Background()); err != nil || n != 2 {
		t.Fatalf("first run: %d, %v", n, err)
	}
	if n := f.count(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND last_error IS NOT NULL AND next_attempt_at > now()`); n != 2 {
		t.Fatalf("events rescheduled after the lost ack = %d", n)
	}
	var ids []string
	rows, _ := f.db.Owner.Query(context.Background(), `SELECT id::text FROM outbox_events ORDER BY seq`)
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	// Bring the retry forward instead of waiting for the backoff.
	if _, err := f.db.Owner.Exec(context.Background(), `UPDATE outbox_events SET next_attempt_at = now()`); err != nil {
		t.Fatal(err)
	}
	client.fail.Store(false)
	if n, err := p.RunOnce(context.Background()); err != nil || n != 2 {
		t.Fatalf("retry: %d, %v", n, err)
	}
	got := f.receivedEventIDs(t, 2)
	for _, id := range ids {
		if got[id] == 0 {
			t.Fatalf("event %s not received; got %v", id, got)
		}
	}
	if len(got) != 2 {
		t.Fatalf("republication produced new event ids: %v", got)
	}
	if n := f.count(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL AND attempts = 2`); n != 2 {
		t.Fatalf("published after 2 attempts = %d", n)
	}
}

type down struct{ calls atomic.Int32 }

func (d *down) PublishBatch(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	d.calls.Add(1)
	return nil, errors.New("sns unavailable (injected)")
}

func TestUnavailableBrokerBacksOff(t *testing.T) {
	f := setup(t)
	f.openWallets(t, 1)
	broker := &down{}
	stop := runLoop(f.publisher("instance-1", broker, 30*time.Second))
	time.Sleep(700 * time.Millisecond)
	stop()
	// With backoff, 700 ms is not enough for a second attempt of the same
	// events: one failed batch, not a hot loop.
	if c := broker.calls.Load(); c != 1 {
		t.Fatalf("publish calls = %d in 700ms", c)
	}
	var attempts int
	var lastError string
	var wait float64
	if err := f.db.Owner.QueryRow(context.Background(), `SELECT max(attempts), max(last_error),
		min(EXTRACT(EPOCH FROM next_attempt_at - now()))::float8 FROM outbox_events`).Scan(&attempts, &lastError, &wait); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || lastError == "" || wait <= 0 {
		t.Fatalf("attempts=%d lastError=%q nextIn=%.2fs", attempts, lastError, wait)
	}
}

func TestPublishedEventsCannotBeReopened(t *testing.T) {
	f := setup(t)
	f.openWallets(t, 1)
	p := f.publisher("instance-1", f.aws.SNS, time.Second)
	if _, err := p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.App.Exec(context.Background(), `UPDATE outbox_events SET published_at = NULL`); err == nil {
		t.Fatal("a published event was reopened")
	}
}
