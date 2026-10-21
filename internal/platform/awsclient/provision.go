package awsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/devfertonani/wallet-service/internal/config"
)

// Delivery limits for the inbound queue. ARCHITECTURE.md explains the numbers.
const (
	VisibilityTimeoutSeconds = 30
	MaxReceiveCount          = 5
	// EventsAuditQueue receives every outbound event through the SNS topic,
	// so the outbox contract can be inspected and tested end to end.
	EventsAuditQueue = "wallet-events-audit.fifo"
)

// Provision creates the queues, the redrive policy, the events topic and its
// audit subscription. Every call is idempotent, so it runs on each startup of
// the local environment.
func Provision(ctx context.Context, sq *sqs.Client, sn *sns.Client, cfg config.Config) error {
	fifo := map[string]string{
		string(sqstypes.QueueAttributeNameFifoQueue):                 "true",
		string(sqstypes.QueueAttributeNameContentBasedDeduplication): "false",
	}

	dlq, err := sq.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(cfg.WagerDLQName),
		Attributes: merge(fifo, map[string]string{string(sqstypes.QueueAttributeNameMessageRetentionPeriod): "1209600"}),
	})
	if err != nil {
		return fmt.Errorf("create %s: %w", cfg.WagerDLQName, err)
	}
	dlqArn, err := queueArn(ctx, sq, aws.ToString(dlq.QueueUrl))
	if err != nil {
		return err
	}

	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": dlqArn,
		"maxReceiveCount":     strconv.Itoa(MaxReceiveCount),
	})
	main := merge(fifo, map[string]string{
		string(sqstypes.QueueAttributeNameVisibilityTimeout): strconv.Itoa(VisibilityTimeoutSeconds),
		string(sqstypes.QueueAttributeNameRedrivePolicy):     string(redrive),
	})
	q, err := sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(cfg.WagerQueueName), Attributes: fifo})
	if err != nil {
		return fmt.Errorf("create %s: %w", cfg.WagerQueueName, err)
	}
	// SetQueueAttributes keeps the redrive policy current on queues that
	// already existed, where CreateQueue with different attributes would fail.
	if _, err := sq.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: without(main, fifo)}); err != nil {
		return fmt.Errorf("configure %s: %w", cfg.WagerQueueName, err)
	}

	topic, err := sn.CreateTopic(ctx, &sns.CreateTopicInput{
		Name:       aws.String(cfg.EventsTopic),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"},
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", cfg.EventsTopic, err)
	}

	audit, err := sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(EventsAuditQueue), Attributes: fifo})
	if err != nil {
		return fmt.Errorf("create %s: %w", EventsAuditQueue, err)
	}
	auditArn, err := queueArn(ctx, sq, aws.ToString(audit.QueueUrl))
	if err != nil {
		return err
	}
	if _, err := sn.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:   topic.TopicArn,
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(auditArn),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	}); err != nil {
		return fmt.Errorf("subscribe %s: %w", EventsAuditQueue, err)
	}
	return nil
}

func queueArn(ctx context.Context, sq *sqs.Client, url string) (string, error) {
	out, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", fmt.Errorf("queue arn %s: %w", url, err)
	}
	return out.Attributes[string(sqstypes.QueueAttributeNameQueueArn)], nil
}

func merge(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func without(m, drop map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if _, ok := drop[k]; !ok {
			out[k] = v
		}
	}
	return out
}
