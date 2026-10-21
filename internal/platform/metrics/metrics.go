// Package metrics owns the Prometheus registry and the business metrics. It
// implements wagering.Recorder, so the use cases do not import Prometheus.
package metrics

import (
	"time"

	"github.com/devfertonani/wallet-service/internal/platform/httpserver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
)

type Metrics struct {
	Registry *prometheus.Registry

	operations           *prometheus.CounterVec
	replays              *prometheus.CounterVec
	idempotencyConflicts *prometheus.CounterVec
	lockConflicts        prometheus.Counter
	duration             *prometheus.HistogramVec
	pendingAttempts      *prometheus.CounterVec
	divergences          prometheus.Counter

	SQSMessages     *prometheus.CounterVec
	SQSRetries      prometheus.Counter
	SQSDLQ          *prometheus.CounterVec
	OutboxPublished *prometheus.CounterVec
	OutboxLag       prometheus.Gauge
	OutboxBacklog   prometheus.Gauge
	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
}

// latencyBuckets go from 1 ms to 10 s.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{Registry: reg}

	m.operations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_operations_total", Help: "Wager operations by kind, outcome status and channel.",
	}, []string{"kind", "status", "channel"})
	m.replays = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_idempotent_replays_total", Help: "Duplicate operations answered from the persisted result.",
	}, []string{"channel"})
	m.idempotencyConflicts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_idempotency_conflicts_total", Help: "Idempotency key or message id reused with another payload, or external id reused with another key.",
	}, []string{"channel"})
	m.lockConflicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wallet_lock_conflicts_total", Help: "Concurrent duplicates that exhausted the retry or version conflicts on the wallet row.",
	})
	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wallet_operation_duration_seconds", Help: "Processing latency of wager operations.", Buckets: latencyBuckets,
	}, []string{"channel"})
	m.pendingAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_pending_reference_attempts_total", Help: "Attempts of the pending reference worker by outcome.",
	}, []string{"outcome"})
	m.divergences = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wallet_reconciliation_divergences_total", Help: "Reconciliations whose stored balance differs from the ledger.",
	})
	m.SQSMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_sqs_messages_total", Help: "Inbound SQS messages by handling result.",
	}, []string{"result"})
	m.SQSRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wallet_sqs_retries_total", Help: "Inbound messages returned to the queue after a transient failure.",
	})
	m.SQSDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_sqs_dlq_total", Help: "Inbound messages sent to the DLQ by the consumer, by reason.",
	}, []string{"reason"})
	m.OutboxPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_outbox_publish_total", Help: "Outbox publication attempts by result.",
	}, []string{"result"})
	m.OutboxLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event.",
	})
	m.OutboxBacklog = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_outbox_backlog", Help: "Unpublished outbox events.",
	})
	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_http_requests_total", Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wallet_http_request_duration_seconds", Help: "HTTP latency by route.", Buckets: latencyBuckets,
	}, []string{"route"})

	for _, c := range []prometheus.Collector{m.operations, m.replays, m.idempotencyConflicts, m.lockConflicts, m.duration,
		m.pendingAttempts, m.divergences, m.SQSMessages, m.SQSRetries, m.SQSDLQ, m.OutboxPublished, m.OutboxLag,
		m.OutboxBacklog, m.HTTPRequests, m.HTTPDuration} {
		f(c)
	}
	// Pre-create the series that alerts look at, so they read 0 instead of
	// "no data" before the first event.
	m.divergences.Add(0)
	m.lockConflicts.Add(0)
	m.SQSRetries.Add(0)
	return m
}

func (m *Metrics) Operation(channel, kind, status string, replay bool, d time.Duration) {
	m.operations.WithLabelValues(kind, status, channel).Inc()
	m.duration.WithLabelValues(channel).Observe(d.Seconds())
	if replay {
		m.replays.WithLabelValues(channel).Inc()
	}
}

func (m *Metrics) IdempotencyConflict(channel string) {
	m.idempotencyConflicts.WithLabelValues(channel).Inc()
}
func (m *Metrics) LockConflict() { m.lockConflicts.Inc() }
func (m *Metrics) PendingReferenceAttempt(outcome string) {
	m.pendingAttempts.WithLabelValues(outcome).Inc()
}
func (m *Metrics) ReconciliationDivergence() { m.divergences.Inc() }

// Route exposes /metrics. It is public locally (ARCHITECTURE.md); in
// production it would live on an internal port.
func (m *Metrics) Route() httpserver.Route {
	return httpserver.Route{Pattern: "GET /metrics", Handler: promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})}
}

var Module = fx.Module("metrics",
	fx.Provide(New),
	fx.Provide(httpserver.AsRoute(func(m *Metrics) httpserver.Route { return m.Route() })),
)
