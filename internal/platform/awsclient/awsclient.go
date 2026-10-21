// Package awsclient builds the SQS and SNS clients. AWS_ENDPOINT_URL points
// them at MiniStack locally; without it they talk to real AWS.
package awsclient

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/devfertonani/wallet-service/internal/config"
	"go.uber.org/fx"
)

func NewAWSConfig(cfg config.Config) (aws.Config, error) {
	ac, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return aws.Config{}, fmt.Errorf("aws: load config: %w", err)
	}
	if cfg.AWSEndpointURL != "" {
		ac.BaseEndpoint = aws.String(cfg.AWSEndpointURL)
	}
	return ac, nil
}

func NewSQS(ac aws.Config) *sqs.Client { return sqs.NewFromConfig(ac) }
func NewSNS(ac aws.Config) *sns.Client { return sns.NewFromConfig(ac) }

// Queues holds the resolved queue URLs and topic ARN. They are resolved in
// OnStart: a missing queue fails the start instead of the first message.
type Queues struct {
	WagerURL    string
	WagerDLQURL string
	EventsTopic string
}

func NewQueues(lc fx.Lifecycle, cfg config.Config, sq *sqs.Client, sn *sns.Client, log *slog.Logger) *Queues {
	q := &Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		if cfg.AWSAutoProvision {
			if err := Provision(ctx, sq, sn, cfg); err != nil {
				return fmt.Errorf("aws: provision: %w", err)
			}
		}
		r, err := Resolve(ctx, sq, sn, cfg)
		if err != nil {
			return err
		}
		*q = r
		log.Info("aws resources resolved", "wagerQueue", q.WagerURL, "eventsTopic", q.EventsTopic)
		return nil
	}})
	return q
}

func Resolve(ctx context.Context, sq *sqs.Client, sn *sns.Client, cfg config.Config) (Queues, error) {
	var q Queues
	for _, it := range []struct {
		name string
		dst  *string
	}{{cfg.WagerQueueName, &q.WagerURL}, {cfg.WagerDLQName, &q.WagerDLQURL}} {
		out, err := sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(it.name)})
		if err != nil {
			return Queues{}, fmt.Errorf("sqs: queue %s: %w", it.name, err)
		}
		*it.dst = aws.ToString(out.QueueUrl)
	}
	// CreateTopic is idempotent and returns the ARN of an existing topic with
	// the same attributes, which is the documented way to look one up by name.
	t, err := sn.CreateTopic(ctx, &sns.CreateTopicInput{
		Name:       aws.String(cfg.EventsTopic),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"},
	})
	if err != nil {
		return Queues{}, fmt.Errorf("sns: topic %s: %w", cfg.EventsTopic, err)
	}
	q.EventsTopic = aws.ToString(t.TopicArn)
	return q, nil
}

var Module = fx.Module("aws",
	fx.Provide(NewAWSConfig, NewSQS, NewSNS, NewQueues),
)
