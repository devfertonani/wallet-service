package target

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
)

// Drainer consumes the outbound events audit queue (the SNS fan-out target)
// while a run is in progress, like a downstream consumer would.
//
// Without it the queue only grows, and MiniStack's cost per operation grows
// with queue depth (measured by "loadgen baseline"), so every SNS publish of
// the outbox gets slower during the run: the test would measure the
// emulator's queue scan, not the service. It also yields the end-to-end event
// delivery latency (event occurredAt to receipt by the consumer).
type Drainer struct {
	client  *sqs.Client
	url     string
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	drained atomic.Int64
	mu      sync.Mutex
	latency *hist.Histogram
	since   time.Time
}

// StartDrainer starts the consumers. Events that occurred before the call
// (for example the opening events of the seed) are consumed but left out of
// the delivery latency.
func StartDrainer(ctx context.Context, client *sqs.Client, url string, workers int) *Drainer {
	ctx, cancel := context.WithCancel(ctx)
	d := &Drainer{client: client, url: url, cancel: cancel, latency: hist.New(), since: time.Now()}
	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go d.loop(ctx)
	}
	return d
}

// Stop ends the consumers and returns how many events were consumed and
// their delivery latency.
func (d *Drainer) Stop() (int64, *hist.Histogram) {
	d.cancel()
	d.wg.Wait()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drained.Load(), d.latency
}

func (d *Drainer) loop(ctx context.Context) {
	defer d.wg.Done()
	for ctx.Err() == nil {
		out, err := d.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(d.url), MaxNumberOfMessages: 10, WaitTimeSeconds: 2})
		if err != nil {
			if ctx.Err() == nil {
				time.Sleep(200 * time.Millisecond)
			}
			continue
		}
		if len(out.Messages) == 0 {
			continue
		}
		now := time.Now()
		del := make([]sqstypes.DeleteMessageBatchRequestEntry, len(out.Messages))
		d.mu.Lock()
		for i, m := range out.Messages {
			del[i] = sqstypes.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle}
			var ev struct {
				OccurredAt time.Time `json:"occurredAt"`
			}
			if json.Unmarshal([]byte(aws.ToString(m.Body)), &ev) == nil && !ev.OccurredAt.Before(d.since) {
				d.latency.Record(now.Sub(ev.OccurredAt))
			}
		}
		d.mu.Unlock()
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := d.client.DeleteMessageBatch(dctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(d.url), Entries: del}); err == nil {
			d.drained.Add(int64(len(del)))
		}
		cancel()
	}
}
