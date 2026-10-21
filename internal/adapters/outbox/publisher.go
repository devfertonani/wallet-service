// Package outbox publishes committed outbox events to SNS. Every instance runs
// a publisher; they share the table through short claims with SKIP LOCKED and
// a lease, so an abandoned claim is taken over after the lease expires.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
	"github.com/devfertonani/wallet-service/internal/platform/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// SNSPublisher is the part of the SNS client the publisher uses.
type SNSPublisher interface {
	PublishBatch(ctx context.Context, in *sns.PublishBatchInput, opts ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

type Publisher struct {
	pool     *pgxpool.Pool
	sns      SNSPublisher
	topicARN func() string
	owner    string
	batch    int
	lease    time.Duration
	poll     time.Duration
	metrics  *metrics.Metrics
	log      *slog.Logger
}

type Options struct {
	Owner     string // instance id, recorded as locked_by
	BatchSize int
	Lease     time.Duration
	Poll      time.Duration
}

func NewPublisher(pool *pgxpool.Pool, client SNSPublisher, topicARN func() string, o Options, m *metrics.Metrics, log *slog.Logger) *Publisher {
	return &Publisher{pool: pool, sns: client, topicARN: topicARN, owner: o.Owner, batch: o.BatchSize, lease: o.Lease,
		poll: o.Poll, metrics: m, log: log.With("component", "outbox")}
}

type event struct {
	id          uuid.UUID
	aggregateID uuid.UUID
	eventType   string
	payload     []byte
	attempts    int
}

// Claim takes up to batch due events in a short transaction and leases them to
// this publisher. Publication happens after the claim commits, so it only ever
// sees events whose originating transaction committed.
func (p *Publisher) Claim(ctx context.Context) ([]event, error) {
	rows, err := p.pool.Query(ctx, `UPDATE outbox_events
		SET locked_by = $1, locked_until = now() + $2::interval, attempts = attempts + 1
		WHERE id IN (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY seq
			FOR UPDATE SKIP LOCKED
			LIMIT $3)
		RETURNING id, aggregate_id, event_type, payload, attempts`, p.owner, fmt.Sprintf("%d milliseconds", p.lease.Milliseconds()), p.batch)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (event, error) {
		var e event
		err := r.Scan(&e.id, &e.aggregateID, &e.eventType, &e.payload, &e.attempts)
		return e, err
	})
}

// RunOnce claims and publishes one batch. It returns how many were claimed.
func (p *Publisher) RunOnce(ctx context.Context) (int, error) {
	evs, err := p.Claim(ctx)
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	// SNS PublishBatch takes at most 10 entries; the chunks of a claim are
	// published in parallel and confirmed with one UPDATE.
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		published []uuid.UUID
	)
	for start := 0; start < len(evs); start += 10 {
		chunk := evs[start:min(start+10, len(evs))]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok := p.publish(ctx, chunk)
			mu.Lock()
			published = append(published, ok...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	p.markPublished(ctx, published)
	return len(evs), nil
}

// publish sends one chunk and returns the ids SNS accepted; failures are
// rescheduled here.
func (p *Publisher) publish(ctx context.Context, evs []event) []uuid.UUID {
	entries := make([]snstypes.PublishBatchRequestEntry, len(evs))
	byID := make(map[string]event, len(evs))
	for i, e := range evs {
		id := e.id.String()
		byID[id] = e
		entries[i] = snstypes.PublishBatchRequestEntry{
			Id:      aws.String(id),
			Message: aws.String(string(e.payload)),
			// Events of one wallet share a group; the event id deduplicates
			// republications within the SNS FIFO window.
			MessageGroupId:         aws.String(e.aggregateID.String()),
			MessageDeduplicationId: aws.String(id),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"eventType": {DataType: aws.String("String"), StringValue: aws.String(e.eventType)},
			},
		}
	}
	out, err := p.sns.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(p.topicARN()), PublishBatchRequestEntries: entries})
	if err != nil {
		for _, e := range evs {
			p.failed(ctx, e, err.Error())
		}
		return nil
	}
	ok := make([]uuid.UUID, 0, len(out.Successful))
	for _, s := range out.Successful {
		if e, found := byID[aws.ToString(s.Id)]; found {
			ok = append(ok, e.id)
		}
	}
	for _, f := range out.Failed {
		if e, found := byID[aws.ToString(f.Id)]; found {
			p.failed(ctx, e, aws.ToString(f.Code)+": "+aws.ToString(f.Message))
		}
	}
	return ok
}

// markPublished confirms the published events. It is conditioned on our
// lease: if it expired and another publisher took an event, that one marks it;
// the event may then have been sent twice with the same eventId, which
// consumers deduplicate.
func (p *Publisher) markPublished(ctx context.Context, ids []uuid.UUID) {
	if len(ids) == 0 {
		return
	}
	_, err := p.pool.Exec(ctx, `UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = ANY($1) AND locked_by = $2 AND published_at IS NULL`, ids, p.owner)
	if err != nil {
		p.log.Warn("outbox mark published failed; the lease will expire and the events will be republished", "events", len(ids), "error", err)
		return
	}
	p.metrics.OutboxPublished.WithLabelValues("published").Add(float64(len(ids)))
}

// Backoff of failed publications: 1 s doubling up to 5 min, with jitter.
func backoff(attempts int) time.Duration {
	d := time.Second << min(attempts, 9)
	d = min(d, 5*time.Minute)
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func (p *Publisher) failed(ctx context.Context, e event, reason string) {
	p.metrics.OutboxPublished.WithLabelValues("failed").Inc()
	next := backoff(e.attempts)
	p.log.Warn("outbox publish failed", "eventId", e.id, "eventType", e.eventType, "attempts", e.attempts, "retryIn", next.String(), "error", reason)
	if len(reason) > 500 {
		reason = reason[:500]
	}
	_, err := p.pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() + $3::interval, locked_by = NULL, locked_until = NULL, last_error = $4
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, e.id, p.owner, strconv.FormatInt(next.Milliseconds(), 10)+" milliseconds", reason)
	if err != nil {
		p.log.Warn("outbox release failed; the lease will expire", "eventId", e.id, "error", err)
	}
}

// UpdateLag refreshes the backlog and the age of the oldest pending event.
func (p *Publisher) UpdateLag(ctx context.Context) error {
	var backlog int64
	var lag float64
	err := p.pool.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(occurred_at)), 0)::float8
		FROM outbox_events WHERE published_at IS NULL`).Scan(&backlog, &lag)
	if err != nil {
		return err
	}
	p.metrics.OutboxBacklog.Set(float64(backlog))
	p.metrics.OutboxLag.Set(lag)
	return nil
}

// Loop publishes until ctx is cancelled. A full batch is followed immediately
// by the next claim; an empty one waits for the poll interval.
func (p *Publisher) Loop(ctx context.Context) {
	lastLag := time.Time{}
	errDelay := p.poll
	for ctx.Err() == nil {
		// Publication of a claimed batch is not interrupted by shutdown:
		// finishing it is quicker than letting the lease expire. It is still
		// bounded, below the lease, so a hung dependency cannot hold the loop.
		work, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.lease/2)
		if time.Since(lastLag) > 2*time.Second {
			if err := p.UpdateLag(work); err != nil {
				p.log.Warn("outbox lag query failed", "error", err)
			}
			lastLag = time.Now()
		}
		n, err := p.RunOnce(work)
		cancel()
		switch {
		case err != nil:
			p.log.Warn("outbox claim failed", "error", err)
			errDelay = min(errDelay*2, 5*time.Second)
			if !worker.Sleep(ctx, errDelay) {
				return
			}
		case n < p.batch:
			errDelay = p.poll
			if !worker.Sleep(ctx, p.poll) {
				return
			}
		}
	}
}

func Register(lc fx.Lifecycle, cfg config.Config, pool *pgxpool.Pool, client *sns.Client, q *awsclient.Queues, m *metrics.Metrics, log *slog.Logger) *Publisher {
	p := NewPublisher(pool, client, func() string { return q.EventsTopic }, Options{
		Owner: cfg.InstanceID, BatchSize: cfg.OutboxBatchSize, Lease: cfg.OutboxLease, Poll: cfg.OutboxPollInterval,
	}, m, log)
	worker.Start(lc, log, "outbox-publisher", 1, p.Loop)
	return p
}

var Module = fx.Module("outbox", fx.Provide(Register), fx.Invoke(func(*Publisher) {}))
