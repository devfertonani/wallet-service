// Package baseline measures the ceiling of the messaging emulator alone, with
// no wallet logic involved: SQS FIFO send and receive+delete on a scratch
// queue, and SNS FIFO publish (single and batch) on the real events topic,
// including the fan-out to its audit queue. If the service's outbox or
// consumer saturate near these numbers, the bottleneck is the broker, not the
// service.
package baseline

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/target"
	"github.com/google/uuid"
)

const scratchQueue = "loadgen-baseline.fifo"

// File is the name of the result in the run directory.
const File = "baseline.json"

type Config struct {
	SQS        *sqs.Client
	SNS        *sns.Client
	TopicName  string
	AuditQueue string
	Duration   time.Duration
	Workers    int
	Groups     int
	Logf       func(string, ...any)
}

type Phase struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Messages    int64           `json:"messages"`
	Calls       int64           `json:"calls"`
	Seconds     float64         `json:"seconds"`
	PerSec      float64         `json:"messagesPerSec"`
	Errors      int64           `json:"errors"`
	FirstError  string          `json:"firstError,omitempty"`
	CallLatency *hist.Histogram `json:"callLatency"`
	// Timeline is messages completed in each second of the phase, to show
	// whether the emulator slows down as its queues grow.
	Timeline []int64 `json:"timeline"`
}

type Result struct {
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
	Workers    int       `json:"workers"`
	Groups     int       `json:"messageGroups"`
	Duration   string    `json:"phaseDuration"`
	Phases     []Phase   `json:"phases"`
	// FanOut is how many SNS publishes reached the audit queue and how long
	// the delivery took after the last publish.
	FanOutPublished int64   `json:"fanOutPublished"`
	FanOutDelivered int64   `json:"fanOutDelivered"`
	FanOutSeconds   float64 `json:"fanOutSeconds"`
}

func NewSNSClient(ctx context.Context, region, endpoint string) (*sns.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return sns.NewFromConfig(cfg, func(o *sns.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

// body is about the size of a real envelope or event (~600 bytes).
var body = `{"messageId":"m","type":"Baseline","occurredAt":"2026-01-01T00:00:00.000Z","data":{"pad":"` + strings.Repeat("x", 520) + `"}}`

type counter struct {
	msgs, calls, errs atomic.Int64
	mu                sync.Mutex
	first             string
	lat               *hist.Histogram
	start             time.Time
	timeline          []int64
}

func (c *counter) done(n int, d time.Duration, err error) {
	c.calls.Add(1)
	c.mu.Lock()
	c.lat.Record(d)
	if err == nil {
		sec := int(time.Since(c.start) / time.Second)
		for len(c.timeline) <= sec {
			c.timeline = append(c.timeline, 0)
		}
		c.timeline[sec] += int64(n)
	}
	if err != nil && c.first == "" {
		c.first = err.Error()
	}
	c.mu.Unlock()
	if err != nil {
		c.errs.Add(1)
		return
	}
	c.msgs.Add(int64(n))
}

// loop runs fn on every worker until the deadline; fn returns how many
// messages it handled.
func (cfg Config) loop(ctx context.Context, name, desc string, until func(c *counter) bool, fn func(ctx context.Context) (int, error)) Phase {
	start := time.Now()
	c := &counter{lat: hist.New(), start: start}
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil && !until(c) {
				t0 := time.Now()
				n, err := fn(ctx)
				c.done(n, time.Since(t0), err)
			}
		}()
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	p := Phase{Name: name, Description: desc, Messages: c.msgs.Load(), Calls: c.calls.Load(), Seconds: secs,
		Errors: c.errs.Load(), FirstError: c.first, CallLatency: c.lat, Timeline: c.timeline}
	if secs > 0 {
		p.PerSec = float64(p.Messages) / secs
	}
	if cfg.Logf != nil {
		cfg.Logf("baseline %s: %d messages in %.1fs = %.0f msg/s (%d errors)", name, p.Messages, secs, p.PerSec, p.Errors)
	}
	return p
}

func (cfg Config) group() string { return "g" + strconv.Itoa(rand.IntN(cfg.Groups)) }

func Run(ctx context.Context, cfg Config) (Result, error) {
	res := Result{StartedAt: time.Now(), Workers: cfg.Workers, Groups: cfg.Groups, Duration: cfg.Duration.String()}
	q, err := cfg.SQS.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(scratchQueue), Attributes: map[string]string{
		string(sqstypes.QueueAttributeNameFifoQueue):                 "true",
		string(sqstypes.QueueAttributeNameContentBasedDeduplication): "false",
	}})
	if err != nil {
		return res, fmt.Errorf("create scratch queue: %w", err)
	}
	qurl := q.QueueUrl
	defer func() {
		_, _ = cfg.SQS.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: qurl})
	}()
	_ = target.Purge(ctx, cfg.SQS, aws.ToString(qurl))

	// 1. SQS send, batches of 10, random message groups (like walletId).
	deadline := time.Now().Add(cfg.Duration)
	send := cfg.loop(ctx, "sqs-send-batch", "SQS SendMessageBatch (10 per call) on a scratch FIFO queue",
		func(*counter) bool { return time.Now().After(deadline) },
		func(ctx context.Context) (int, error) {
			entries := make([]sqstypes.SendMessageBatchRequestEntry, 10)
			for i := range entries {
				entries[i] = sqstypes.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), MessageBody: aws.String(body),
					MessageGroupId: aws.String(cfg.group()), MessageDeduplicationId: aws.String(uuid.NewString())}
			}
			out, err := cfg.SQS.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: qurl, Entries: entries})
			if err != nil {
				return 0, err
			}
			if len(out.Failed) > 0 {
				return len(out.Successful), fmt.Errorf("%d entries failed: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
			}
			return len(out.Successful), nil
		})
	res.Phases = append(res.Phases, send)

	// 2. Receive + delete everything that was sent, like a consumer.
	recvDeadline := time.Now().Add(3 * cfg.Duration)
	var empty atomic.Int64
	recv := cfg.loop(ctx, "sqs-receive-delete", "SQS ReceiveMessage (up to 10) + DeleteMessageBatch, until the queue is empty",
		func(c *counter) bool {
			return c.msgs.Load() >= send.Messages || time.Now().After(recvDeadline) || empty.Load() > int64(3*cfg.Workers)
		},
		func(ctx context.Context) (int, error) {
			out, err := cfg.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qurl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60})
			if err != nil {
				return 0, err
			}
			if len(out.Messages) == 0 {
				empty.Add(1)
				return 0, nil
			}
			empty.Store(0)
			del := make([]sqstypes.DeleteMessageBatchRequestEntry, len(out.Messages))
			for i, m := range out.Messages {
				del[i] = sqstypes.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle}
			}
			if _, err := cfg.SQS.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: qurl, Entries: del}); err != nil {
				return 0, err
			}
			return len(out.Messages), nil
		})
	res.Phases = append(res.Phases, recv)

	// 3 and 4. SNS publish to the real FIFO topic, which fans out to the audit queue.
	topic, err := cfg.SNS.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(cfg.TopicName),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		return res, fmt.Errorf("resolve topic %s: %w", cfg.TopicName, err)
	}
	auditURL, err := target.QueueURL(ctx, cfg.SQS, cfg.AuditQueue)
	if err != nil {
		return res, err
	}
	_ = target.Purge(ctx, cfg.SQS, auditURL)
	before, _ := target.QueueDepth(ctx, cfg.SQS, auditURL)
	attr := map[string]snstypes.MessageAttributeValue{"eventType": {DataType: aws.String("String"), StringValue: aws.String("Baseline")}}

	deadline = time.Now().Add(cfg.Duration)
	single := cfg.loop(ctx, "sns-publish", "SNS Publish (one message per call) on the wallet-events.fifo topic",
		func(*counter) bool { return time.Now().After(deadline) },
		func(ctx context.Context) (int, error) {
			_, err := cfg.SNS.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(body),
				MessageGroupId: aws.String(cfg.group()), MessageDeduplicationId: aws.String(uuid.NewString()), MessageAttributes: attr})
			if err != nil {
				return 0, err
			}
			return 1, nil
		})
	res.Phases = append(res.Phases, single)

	deadline = time.Now().Add(cfg.Duration)
	batch := cfg.loop(ctx, "sns-publish-batch", "SNS PublishBatch (10 per call) on the wallet-events.fifo topic",
		func(*counter) bool { return time.Now().After(deadline) },
		func(ctx context.Context) (int, error) {
			entries := make([]snstypes.PublishBatchRequestEntry, 10)
			for i := range entries {
				entries[i] = snstypes.PublishBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), Message: aws.String(body),
					MessageGroupId: aws.String(cfg.group()), MessageDeduplicationId: aws.String(uuid.NewString()), MessageAttributes: attr}
			}
			out, err := cfg.SNS.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: topic.TopicArn, PublishBatchRequestEntries: entries})
			if err != nil {
				return 0, err
			}
			if len(out.Failed) > 0 {
				return len(out.Successful), fmt.Errorf("%d entries failed: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
			}
			return len(out.Successful), nil
		})
	res.Phases = append(res.Phases, batch)

	// 5. Fan-out: wait until the audit queue stops growing.
	res.FanOutPublished = single.Messages + batch.Messages
	fanStart := time.Now()
	last, stableSince := int64(-1), time.Now()
	for time.Since(fanStart) < 60*time.Second {
		d, err := target.QueueDepth(ctx, cfg.SQS, auditURL)
		if err == nil && d != last {
			last, stableSince = d, time.Now()
		}
		if last-before >= res.FanOutPublished || time.Since(stableSince) > 3*time.Second {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	res.FanOutDelivered = last - before
	res.FanOutSeconds = stableSince.Sub(fanStart).Seconds()
	_ = target.Purge(ctx, cfg.SQS, auditURL) // synthetic events must not stay in the audit queue
	res.FinishedAt = time.Now()
	return res, nil
}
