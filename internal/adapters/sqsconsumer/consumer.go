// Package sqsconsumer consumes wager-transactions.fifo. Each message goes
// through the same use case as HTTP, with the inbox registered and completed
// in the same SQL transaction; the message is deleted only after that commit.
package sqsconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
	"github.com/devfertonani/wallet-service/internal/platform/worker"
	"go.uber.org/fx"
)

// ConsumerName identifies this consumer in the inbox.
const ConsumerName = "wallet.wager-transactions"

// MessageType is the only envelope type accepted on the queue.
const MessageType = "WagerTransactionRequested"

// SQS is the part of the SQS client the consumer uses.
type SQS interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type Consumer struct {
	svc     *wagering.Service
	sqs     SQS
	queues  func() awsclient.Queues
	wait    time.Duration
	timeout time.Duration
	metrics *metrics.Metrics
	log     *slog.Logger
	// reprovision recreates missing queues (local emulator only); nil otherwise.
	reprovision func(context.Context) error
	senders     map[string]string
}

type Options struct {
	WaitTime       time.Duration
	MessageTimeout time.Duration
	Reprovision    func(context.Context) error
	// ProviderSenders maps an SQS SenderId to the only provider it may send
	// operations for. Empty disables the check.
	ProviderSenders map[string]string
}

func New(svc *wagering.Service, client SQS, queues func() awsclient.Queues, o Options, m *metrics.Metrics, log *slog.Logger) *Consumer {
	return &Consumer{svc: svc, sqs: client, queues: queues, wait: o.WaitTime, timeout: o.MessageTimeout,
		reprovision: o.Reprovision, senders: o.ProviderSenders, metrics: m, log: log.With("component", "sqs-consumer")}
}

// Envelope is the inbound message body.
type Envelope struct {
	MessageID  string      `json:"messageId"`
	Type       string      `json:"type"`
	OccurredAt string      `json:"occurredAt"`
	Data       MessageData `json:"data"`
}

type MessageData struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	IdempotencyKey                 string     `json:"idempotencyKey"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          *money.DTO `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

// Outcome of handling one message.
type Outcome string

const (
	Done      Outcome = "done"      // committed (processed, rejected or pending): delete
	Retry     Outcome = "retry"     // transient: return to the queue with backoff
	Permanent Outcome = "permanent" // can never succeed: send to the DLQ and delete
)

// permanentError carries the DLQ reason.
type permanentError struct{ reason string }

func (e permanentError) Error() string { return e.reason }

// parse validates the envelope and builds the command. Every error is permanent.
func parse(body string) (Envelope, wager.Command, error) {
	// Unknown envelope fields are ignored so producers can add metadata;
	// the business fields are validated by the same command as HTTP.
	var env Envelope
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&env); err != nil {
		return env, wager.Command{}, permanentError{"INVALID_MESSAGE"}
	}
	if env.Type != MessageType {
		return env, wager.Command{}, permanentError{"UNSUPPORTED_MESSAGE_TYPE"}
	}
	if env.MessageID == "" || len(env.MessageID) > wager.MaxIdentifierLength {
		return env, wager.Command{}, permanentError{"INVALID_MESSAGE"}
	}
	if env.Data.Money == nil {
		return env, wager.Command{}, permanentError{string(failure.InvalidRequest)}
	}
	d := env.Data
	cmd, err := wager.NewCommand(wager.CommandInput{
		ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, IdempotencyKey: d.IdempotencyKey,
		PlayerID: d.PlayerID, WalletID: d.WalletID, RoundID: d.RoundID, GameID: d.GameID, Kind: d.Kind,
		Amount: d.Money.Amount, Currency: d.Money.Currency, ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
	})
	if err != nil {
		code, _ := failure.CodeOf(err)
		return env, wager.Command{}, permanentError{string(code)}
	}
	return env, cmd, nil
}

// Handle processes one message body and decides what to do with it. sender
// is the SQS SenderId of the message.
func (c *Consumer) Handle(ctx context.Context, body, sender string) (Outcome, string, error) {
	env, cmd, err := parse(body)
	var pe permanentError
	if errors.As(err, &pe) {
		return Permanent, pe.reason, err
	}
	// The broker policy decides who may write to the queue; this decides for
	// which provider each writer may speak.
	if len(c.senders) > 0 && c.senders[sender] != cmd.ProviderID() {
		return Permanent, "PROVIDER_NOT_AUTHORIZED", fmt.Errorf("sender %q may not send operations of provider %q", sender, cmd.ProviderID())
	}
	res, err := c.svc.Submit(ctx, cmd, wagering.Source{
		Channel: wagering.ChannelSQS, CorrelationID: env.MessageID, CausationID: env.MessageID,
		Inbox: &wagering.InboxMessage{Consumer: ConsumerName, MessageID: env.MessageID},
	})
	switch {
	case err == nil:
		c.log.InfoContext(ctx, "wager message handled", "messageId", env.MessageID, "transactionId", res.Transaction.ID,
			"walletId", res.Transaction.WalletID, "providerId", res.Transaction.ProviderID, "kind", res.Transaction.Kind,
			"status", res.Transaction.Status, "failureCode", res.Transaction.FailureCode, "replay", res.Replay)
		return Done, "", nil
	case errors.Is(err, wagering.ErrIdempotencyKeyReused):
		return Permanent, "IDEMPOTENCY_KEY_REUSED", err
	case errors.Is(err, wagering.ErrDuplicateExternal):
		return Permanent, "DUPLICATE_EXTERNAL_TRANSACTION", err
	case errors.Is(err, wagering.ErrMessageIDReused):
		return Permanent, "MESSAGE_ID_REUSED", err
	case errors.Is(err, wagering.ErrIntegrity):
		return Permanent, string(failure.InvariantViolation), err
	case failure.Is(err, failure.WalletNotFound):
		// The wallet may be opened later; retry, and let the redrive policy
		// move the message to the DLQ after maxReceiveCount.
		return Retry, "WALLET_NOT_FOUND", err
	}
	var fe *failure.Error
	if errors.As(err, &fe) {
		return Permanent, string(fe.Code), err
	}
	return Retry, "TRANSIENT", err
}

// Loop receives and handles messages until ctx is cancelled.
func (c *Consumer) Loop(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		q := c.queues()
		out, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(q.WagerURL),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             int32(c.wait.Seconds()),
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("sqs receive failed", "error", err, "retryIn", delay.String())
			var missing *sqstypes.QueueDoesNotExist
			if errors.As(err, &missing) && c.reprovision != nil {
				if perr := c.reprovision(ctx); perr != nil {
					c.log.Warn("sqs reprovision failed", "error", perr)
				}
			}
			if !worker.Sleep(ctx, delay) {
				return
			}
			delay = min(delay*2, 10*time.Second)
			continue
		}
		delay = time.Second
		c.handleBatch(ctx, q, out.Messages)
	}
}

// handleBatch handles messages in order. A FIFO batch may hold several
// messages of the same group; once one of them is returned to the queue, the
// rest of that group is returned too, so the group keeps its order. On
// shutdown, the in-flight message finishes within its deadline and the ones not
// started are released for immediate redelivery.
func (c *Consumer) handleBatch(ctx context.Context, q awsclient.Queues, msgs []sqstypes.Message) {
	blocked := map[string]bool{}
	for _, m := range msgs {
		group := m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)]
		if ctx.Err() != nil || blocked[group] {
			c.changeVisibility(q, m, 0)
			continue
		}
		// The deadline is detached from shutdown: a started message is
		// finished, not abandoned halfway. It stays below the visibility
		// timeout, so no other consumer sees it while it runs.
		mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
		outcome, reason, err := c.Handle(mctx, aws.ToString(m.Body), m.Attributes[string(sqstypes.MessageSystemAttributeNameSenderId)])
		cancel()
		log := c.log.With("sqsMessageId", aws.ToString(m.MessageId), "group", group)
		c.metrics.SQSMessages.WithLabelValues(string(outcome)).Inc()
		switch outcome {
		case Done:
			c.delete(q, m)
		case Retry:
			blocked[group] = true
			receives, _ := strconv.Atoi(m.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)])
			backoff := min(1<<min(receives, 5), 30) // 2^receiveCount seconds, at most 30
			log.Warn("sqs message will be retried", "reason", reason, "receiveCount", receives, "retryInSeconds", backoff, "error", err)
			c.metrics.SQSRetries.Inc()
			c.changeVisibility(q, m, int32(backoff))
		case Permanent:
			log.Error("sqs message sent to DLQ", "reason", reason, "error", err)
			c.toDLQ(q, m, reason)
		}
	}
}

func (c *Consumer) delete(q awsclient.Queues, m sqstypes.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(q.WagerURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
		// The commit is durable; the redelivery will be recognized by the inbox.
		c.log.Warn("sqs delete failed; the message will be redelivered and deduplicated", "sqsMessageId", aws.ToString(m.MessageId), "error", err)
	}
}

func (c *Consumer) changeVisibility(q awsclient.Queues, m sqstypes.Message, seconds int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(q.WagerURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: seconds,
	}); err != nil {
		c.log.Warn("sqs change visibility failed; the message reappears after the visibility timeout", "sqsMessageId", aws.ToString(m.MessageId), "error", err)
	}
}

// toDLQ copies the message to the DLQ with the reason and deletes it from the
// main queue, without spending the remaining receives. If the process stops
// between the two calls, the message is redelivered and may reach the DLQ twice.
func (c *Consumer) toDLQ(q awsclient.Queues, m sqstypes.Message, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	group := m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	_, err := c.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(q.WagerDLQURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"reason":        {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"sourceQueue":   {DataType: aws.String("String"), StringValue: aws.String(q.WagerURL)},
			"failedAt":      {DataType: aws.String("String"), StringValue: aws.String(time.Now().UTC().Format(time.RFC3339))},
			"originalMsgId": {DataType: aws.String("String"), StringValue: m.MessageId},
		},
	})
	if err != nil {
		c.log.Error("sqs send to DLQ failed; the message stays in the queue and the redrive policy applies", "sqsMessageId", aws.ToString(m.MessageId), "error", err)
		return
	}
	c.metrics.SQSDLQ.WithLabelValues(reason).Inc()
	c.delete(q, m)
}

func Register(lc fx.Lifecycle, cfg config.Config, svc *wagering.Service, client *sqs.Client, snsClient *sns.Client, q *awsclient.Queues, m *metrics.Metrics, log *slog.Logger) *Consumer {
	var reprovision func(context.Context) error
	if cfg.AWSAutoProvision {
		reprovision = func(ctx context.Context) error { return awsclient.Provision(ctx, client, snsClient, cfg) }
	}
	c := New(svc, client, func() awsclient.Queues { return *q }, Options{
		WaitTime: cfg.SQSWaitTime, MessageTimeout: cfg.SQSMessageTimeout, Reprovision: reprovision, ProviderSenders: cfg.SQSProviderSenders,
	}, m, log)
	worker.Start(lc, log, "sqs-consumer", cfg.SQSWorkers, c.Loop)
	return c
}

var Module = fx.Module("sqsconsumer", fx.Provide(Register), fx.Invoke(func(*Consumer) {}))
