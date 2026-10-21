// Package awstest provisions isolated SQS queues and an SNS topic in the real
// emulator started by docker compose (MiniStack), with names unique to each
// test, so tests never read each other's messages.
package awstest

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
	"github.com/devfertonani/wallet-service/internal/testsupport/dotenv"
	"github.com/google/uuid"
)

const defaultEndpoint = "http://localhost:4566"

type Env struct {
	Config config.Config
	SQS    *sqs.Client
	SNS    *sns.Client
	Queues awsclient.Queues
	// EventsQueueURL receives every event published to the test topic.
	EventsQueueURL string
}

// New provisions <prefix>-wager.fifo, its DLQ and topic, plus a queue
// subscribed to the topic, all suffixed with a random id.
func New(t testing.TB) *Env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dotenv.Load()
	endpoint := os.Getenv("TEST_AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	suffix := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	cfg := config.Config{
		AWSRegion: "us-east-1", AWSEndpointURL: endpoint,
		WagerQueueName: "test-wager-" + suffix + ".fifo",
		WagerDLQName:   "test-wager-dlq-" + suffix + ".fifo",
		EventsTopic:    "test-events-" + suffix + ".fifo",
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("000000000000", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	ac.BaseEndpoint = aws.String(endpoint)
	e := &Env{Config: cfg, SQS: sqs.NewFromConfig(ac), SNS: sns.NewFromConfig(ac)}
	if err := awsclient.Provision(ctx, e.SQS, e.SNS, cfg); err != nil {
		t.Fatalf("awstest: provision (is `docker compose up -d ministack` running?): %v", err)
	}
	if e.Queues, err = awsclient.Resolve(ctx, e.SQS, e.SNS, cfg); err != nil {
		t.Fatal(err)
	}
	events, err := e.SQS.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("test-events-sub-" + suffix + ".fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.EventsQueueURL = aws.ToString(events.QueueUrl)
	attrs, err := e.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: events.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.SNS.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(e.Queues.EventsTopic), Protocol: aws.String("sqs"),
		Endpoint: aws.String(attrs.Attributes["QueueArn"]), Attributes: map[string]string{"RawMessageDelivery": "true"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, u := range []string{e.Queues.WagerURL, e.Queues.WagerDLQURL, e.EventsQueueURL} {
			_, _ = e.SQS.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
		_, _ = e.SNS.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(e.Queues.EventsTopic)})
	})
	return e
}

// Send puts a message on the wager queue, grouped by wallet.
func (e *Env) Send(t testing.TB, body any, group, dedupID string) {
	t.Helper()
	b, ok := body.(string)
	if !ok {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		b = string(raw)
	}
	if _, err := e.SQS.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(e.Queues.WagerURL), MessageBody: aws.String(b),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedupID),
	}); err != nil {
		t.Fatal(err)
	}
}

// Drain receives everything currently visible on a queue, deleting it.
func (e *Env) Drain(t testing.TB, queueURL string, wait time.Duration) []sqstypes.Message {
	t.Helper()
	var all []sqstypes.Message
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		out, err := e.SQS.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			all = append(all, m)
			_, _ = e.SQS.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return all
}

// Depth returns visible plus in-flight messages of a queue.
func (e *Env) Depth(t testing.TB, queueURL string) int {
	t.Helper()
	out, err := e.SQS.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, k := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"} {
		v, _ := strconv.Atoi(out.Attributes[k])
		n += v
	}
	return n
}
