package scenario

import (
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the generator's own live metrics, scraped by Prometheus during
// a run so the dashboard can show client-side latency next to the server's.
type Metrics struct {
	Registry   *prometheus.Registry
	Ops        *prometheus.CounterVec
	Latency    *prometheus.HistogramVec
	Scheduled  prometheus.Counter
	Dropped    prometheus.Counter
	Retried503 prometheus.Counter
	TargetRate prometheus.Gauge
	DB         *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		Ops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "loadgen_ops_total", Help: "Operations by channel, kind and final outcome as seen by the client.",
		}, []string{"channel", "kind", "outcome"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "loadgen_op_duration_seconds",
			Help:    "Client latency from the scheduled instant to the final answer (includes retries).",
			Buckets: []float64{.002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5, 10, 30},
		}, []string{"channel", "kind"}),
		Scheduled:  prometheus.NewCounter(prometheus.CounterOpts{Name: "loadgen_tasks_scheduled_total", Help: "Task arrivals scheduled."}),
		Dropped:    prometheus.NewCounter(prometheus.CounterOpts{Name: "loadgen_tasks_dropped_total", Help: "Arrivals dropped because the generator hit its in-flight limit."}),
		Retried503: prometheus.NewCounter(prometheus.CounterOpts{Name: "loadgen_http_503_retried_total", Help: "503 answers retried with the same Idempotency-Key."}),
		TargetRate: prometheus.NewGauge(prometheus.GaugeOpts{Name: "loadgen_target_ops_per_second", Help: "Target operation rate."}),
		DB: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "loadgen_db_gauge", Help: "Database and queue state sampled by the generator every 2 s.",
		}, []string{"name"}),
	}
	m.Registry.MustRegister(m.Ops, m.Latency, m.Scheduled, m.Dropped, m.Retried503, m.TargetRate, m.DB)
	return m
}

func (m *Metrics) Observe(channel, kind, outcome string, d time.Duration) {
	m.Ops.WithLabelValues(channel, kind, outcome).Inc()
	m.Latency.WithLabelValues(channel, kind).Observe(d.Seconds())
}

func (m *Metrics) ObserveDB(s result.DBSample) {
	m.DB.WithLabelValues("outbox_backlog").Set(float64(s.OutboxBacklog))
	m.DB.WithLabelValues("outbox_oldest_seconds").Set(s.OutboxOldestSec)
	m.DB.WithLabelValues("pending_references").Set(float64(s.PendingRefs))
	m.DB.WithLabelValues("lock_waits").Set(float64(s.LockWaits))
	m.DB.WithLabelValues("connections").Set(float64(s.Connections))
	m.DB.WithLabelValues("queue_depth").Set(float64(s.QueueDepth))
}
