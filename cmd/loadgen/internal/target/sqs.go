package target

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// NewSQSClient builds a client for MiniStack (endpoint set) or real AWS.
func NewSQSClient(ctx context.Context, region, endpoint string) (*sqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

func QueueURL(ctx context.Context, c *sqs.Client, name string) (string, error) {
	out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("resolve queue %s: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}

// QueueDepth returns visible plus in-flight messages of a queue.
func QueueDepth(ctx context.Context, c *sqs.Client, url string) (int64, error) {
	out, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, k := range []sqstypes.QueueAttributeName{
		sqstypes.QueueAttributeNameApproximateNumberOfMessages,
		sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
	} {
		n, _ := strconv.ParseInt(out.Attributes[string(k)], 10, 64)
		total += n
	}
	return total, nil
}

// Producer sends to a FIFO queue with SendMessageBatch (up to 10 entries per
// call). Send blocks until the broker acknowledged that message, so a caller
// that sends the operations of one round in sequence keeps their order
// inside the MessageGroupId.
type Producer struct {
	client *sqs.Client
	url    string
	reqs   chan *sendReq
	linger time.Duration
	wg     sync.WaitGroup

	batches atomic.Int64
	retries atomic.Int64
}

type sendReq struct {
	group, dedup, body string
	done               chan error
}

func NewProducer(client *sqs.Client, url string, workers int, linger time.Duration) *Producer {
	p := &Producer{client: client, url: url, reqs: make(chan *sendReq, 4096), linger: linger}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.loop()
	}
	return p
}

func (p *Producer) Batches() int64 { return p.batches.Load() }
func (p *Producer) Retries() int64 { return p.retries.Load() }

// Close stops the workers after the queued messages were sent.
func (p *Producer) Close() {
	close(p.reqs)
	p.wg.Wait()
}

func (p *Producer) Send(ctx context.Context, group, dedup, body string) error {
	r := &sendReq{group: group, dedup: dedup, body: body, done: make(chan error, 1)}
	select {
	case p.reqs <- r:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-r.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) loop() {
	defer p.wg.Done()
	for first := range p.reqs {
		batch := []*sendReq{first}
		timer := time.NewTimer(p.linger)
	fill:
		for len(batch) < 10 {
			select {
			case r, ok := <-p.reqs:
				if !ok {
					break fill
				}
				batch = append(batch, r)
			case <-timer.C:
				break fill
			}
		}
		timer.Stop()
		p.flush(batch)
	}
}

func (p *Producer) flush(batch []*sendReq) {
	pending := map[string]*sendReq{}
	for i, r := range batch {
		pending["m"+strconv.Itoa(i)] = r
	}
	var lastErr error
	for attempt := 0; attempt < 5 && len(pending) > 0; attempt++ {
		if attempt > 0 {
			p.retries.Add(1)
			time.Sleep(time.Duration(50<<attempt) * time.Millisecond)
		}
		entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(pending))
		for id, r := range pending {
			entries = append(entries, sqstypes.SendMessageBatchRequestEntry{
				Id:                     aws.String(id),
				MessageBody:            aws.String(r.body),
				MessageGroupId:         aws.String(r.group),
				MessageDeduplicationId: aws.String(r.dedup),
			})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(p.url), Entries: entries})
		cancel()
		p.batches.Add(1)
		if err != nil {
			lastErr = err
			continue
		}
		for _, ok := range out.Successful {
			id := aws.ToString(ok.Id)
			if r := pending[id]; r != nil {
				r.done <- nil
				delete(pending, id)
			}
		}
		for _, f := range out.Failed {
			id := aws.ToString(f.Id)
			lastErr = fmt.Errorf("sqs entry rejected: %s %s", aws.ToString(f.Code), aws.ToString(f.Message))
			if f.SenderFault {
				if r := pending[id]; r != nil {
					r.done <- lastErr
					delete(pending, id)
				}
			}
		}
	}
	if lastErr == nil {
		lastErr = errors.New("sqs: entry missing from batch response")
	}
	for _, r := range pending {
		r.done <- lastErr
	}
}

// Purge empties a queue; used by the reset script between runs.
func Purge(ctx context.Context, c *sqs.Client, url string) error {
	_, err := c.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(url)})
	return err
}
