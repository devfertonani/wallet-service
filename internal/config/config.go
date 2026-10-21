// Package config loads and validates the process configuration from the
// environment. Invalid configuration fails the Fx start, before any listener
// or worker begins accepting work.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/fx"
)

type Config struct {
	InstanceID      string
	HTTPAddr        string
	ShutdownTimeout time.Duration
	// RequestTimeout bounds the work of one HTTP request, database included.
	RequestTimeout time.Duration

	DatabaseURL      string
	DBMaxConns       int32
	DBConnectTimeout time.Duration
	// DBLockTimeout bounds the wait for a wallet row lock; exceeding it is a
	// transient failure (503 or message returned to the queue).
	DBLockTimeout time.Duration

	AWSRegion      string
	AWSEndpointURL string // empty means real AWS
	WagerQueueName string
	WagerDLQName   string
	EventsTopic    string
	// AWSAutoProvision creates the queues and topic at start (idempotent).
	// Meant for the local emulator, which loses them when it restarts.
	AWSAutoProvision bool

	SQSWorkers int
	// SQSProviderSenders binds a provider to the SQS SenderId (IAM principal)
	// allowed to send its operations: "senderId=providerId,...". Empty
	// disables the check (the local emulator reports one fixed SenderId).
	SQSProviderSenders map[string]string
	SQSWaitTime        time.Duration
	SQSMessageTimeout  time.Duration

	OutboxBatchSize    int
	OutboxPollInterval time.Duration
	OutboxLease        time.Duration

	PendingBaseDelay    time.Duration
	PendingMaxDelay     time.Duration
	PendingTTL          time.Duration
	PendingPollInterval time.Duration

	OIDCIssuerURL string
	OIDCJWKSURL   string
	OIDCAudience  string
	// OIDCClockSkew is the leeway on exp/nbf/iat for clock drift between the
	// IdP and the instances.
	OIDCClockSkew time.Duration
}

// Load reads and validates the whole configuration of the service.
func Load() (Config, error) {
	c, err := Read()
	if err != nil {
		return c, err
	}
	return c, c.validate()
}

// Read parses the environment without the service-level validation, for
// commands such as provision that need only part of it.
func Read() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		InstanceID:     env("INSTANCE_ID", host),
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		AWSRegion:      env("AWS_REGION", "us-east-1"),
		AWSEndpointURL: os.Getenv("AWS_ENDPOINT_URL"),
		WagerQueueName: env("WAGER_QUEUE_NAME", "wager-transactions.fifo"),
		WagerDLQName:   env("WAGER_DLQ_NAME", "wager-transactions-dlq.fifo"),
		EventsTopic:    env("EVENTS_TOPIC_NAME", "wallet-events.fifo"),
		OIDCIssuerURL:  os.Getenv("OIDC_ISSUER_URL"),
		OIDCJWKSURL:    os.Getenv("OIDC_JWKS_URL"),
		OIDCAudience:   env("OIDC_AUDIENCE", "wallet-api"),
	}

	var errs []error
	var err error
	if c.ShutdownTimeout, err = duration("SHUTDOWN_TIMEOUT", 25*time.Second); err != nil {
		errs = append(errs, err)
	}
	if c.DBConnectTimeout, err = duration("DB_CONNECT_TIMEOUT", 5*time.Second); err != nil {
		errs = append(errs, err)
	}
	maxConns, err := integer("DB_MAX_CONNS", 10)
	if err != nil {
		errs = append(errs, err)
	}
	c.DBMaxConns = int32(maxConns)
	if c.SQSProviderSenders, err = pairs("SQS_PROVIDER_SENDERS"); err != nil {
		errs = append(errs, err)
	}
	if c.AWSAutoProvision, err = boolean("AWS_AUTO_PROVISION", false); err != nil {
		errs = append(errs, err)
	}
	for _, d := range []struct {
		key string
		dst *time.Duration
		def time.Duration
	}{
		{"DB_LOCK_TIMEOUT", &c.DBLockTimeout, 3 * time.Second},
		{"HTTP_REQUEST_TIMEOUT", &c.RequestTimeout, 10 * time.Second},
		{"OIDC_CLOCK_SKEW", &c.OIDCClockSkew, 30 * time.Second},
		{"SQS_WAIT_TIME", &c.SQSWaitTime, 10 * time.Second},
		{"SQS_MESSAGE_TIMEOUT", &c.SQSMessageTimeout, 10 * time.Second},
		{"OUTBOX_POLL_INTERVAL", &c.OutboxPollInterval, 200 * time.Millisecond},
		{"OUTBOX_LEASE", &c.OutboxLease, 30 * time.Second},
		{"PENDING_BASE_DELAY", &c.PendingBaseDelay, time.Second},
		{"PENDING_MAX_DELAY", &c.PendingMaxDelay, time.Minute},
		{"PENDING_TTL", &c.PendingTTL, 10 * time.Minute},
		{"PENDING_POLL_INTERVAL", &c.PendingPollInterval, 500 * time.Millisecond},
	} {
		if *d.dst, err = duration(d.key, d.def); err != nil {
			errs = append(errs, err)
		}
	}
	for _, n := range []struct {
		key string
		dst *int
		def int
	}{
		{"SQS_WORKERS", &c.SQSWorkers, 4},
		{"OUTBOX_BATCH_SIZE", &c.OutboxBatchSize, 50},
	} {
		if *n.dst, err = integer(n.key, n.def); err != nil {
			errs = append(errs, err)
		}
	}
	return c, errors.Join(errs...)
}

func (c Config) validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.OIDCIssuerURL == "" {
		errs = append(errs, errors.New("OIDC_ISSUER_URL is required"))
	} else if _, err := url.ParseRequestURI(c.OIDCIssuerURL); err != nil {
		errs = append(errs, fmt.Errorf("OIDC_ISSUER_URL: %w", err))
	}
	if c.OIDCJWKSURL == "" {
		errs = append(errs, errors.New("OIDC_JWKS_URL is required"))
	}
	if c.DBMaxConns < 2 {
		errs = append(errs, errors.New("DB_MAX_CONNS must be at least 2"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be positive"))
	}
	if c.SQSWorkers < 1 || c.OutboxBatchSize < 1 || c.OutboxBatchSize > 1000 {
		errs = append(errs, errors.New("SQS_WORKERS must be >= 1 and OUTBOX_BATCH_SIZE between 1 and 1000"))
	}
	// The SQS visibility timeout is 30 s: a message must finish well before
	// it becomes visible again to another consumer.
	if c.SQSMessageTimeout <= 0 || c.SQSMessageTimeout >= 30*time.Second {
		errs = append(errs, errors.New("SQS_MESSAGE_TIMEOUT must be positive and below the 30s visibility timeout"))
	}
	if c.SQSWaitTime < 0 || c.SQSWaitTime > 20*time.Second {
		errs = append(errs, errors.New("SQS_WAIT_TIME must be between 0 and 20s"))
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{{"DB_LOCK_TIMEOUT", c.DBLockTimeout}, {"HTTP_REQUEST_TIMEOUT", c.RequestTimeout}, {"OUTBOX_POLL_INTERVAL", c.OutboxPollInterval}, {"OUTBOX_LEASE", c.OutboxLease},
		{"PENDING_BASE_DELAY", c.PendingBaseDelay}, {"PENDING_MAX_DELAY", c.PendingMaxDelay}, {"PENDING_TTL", c.PendingTTL},
		{"PENDING_POLL_INTERVAL", c.PendingPollInterval}} {
		if d.v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", d.name))
		}
	}
	if c.PendingMaxDelay < c.PendingBaseDelay {
		errs = append(errs, errors.New("PENDING_MAX_DELAY must be >= PENDING_BASE_DELAY"))
	}
	return errors.Join(errs...)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func integer(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

// pairs parses "k=v,k2=v2".
func pairs(key string) (map[string]string, error) {
	out := map[string]string{}
	v := os.Getenv(key)
	if v == "" {
		return out, nil
	}
	for _, kv := range strings.Split(v, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if !ok || k == "" || val == "" {
			return nil, fmt.Errorf("%s: invalid pair %q", key, kv)
		}
		out[k] = val
	}
	return out, nil
}

func boolean(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

var Module = fx.Module("config", fx.Provide(Load))
