// Package result holds the files each step writes into the run directory:
// seed.json, run.json and verify.json. The report step reads them back.
package result

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
)

const (
	WalletsFile = "wallets.json"
	JournalFile = "journal.ndjson"
	SeedFile    = "seed.json"
	RunFile     = "run.json"
	VerifyFile  = "verify.json"
	SummaryFile = "summary.json"
	ReportFile  = "report.md"
	EnvFile     = "env.txt"
	EventsFile  = "events.ndjson"
	PGSSFile    = "pg_stat_statements.txt"
	CommandFile = "command.txt"
)

type Seed struct {
	Scenario    string          `json:"scenario"`
	Wallets     int             `json:"wallets"`
	Hot         int             `json:"hotWallets"`
	Failed      int             `json:"failed"`
	Seconds     float64         `json:"seconds"`
	PerSecond   float64         `json:"perSecond"`
	Concurrency int             `json:"concurrency"`
	Latency     *hist.Histogram `json:"latency"`
}

type Step struct {
	Step         int             `json:"step"`
	TargetOpsSec float64         `json:"targetOpsPerSec"`
	Ops          int64           `json:"ops"`
	Errors       int64           `json:"errors"`
	Latency      *hist.Histogram `json:"latency"`
}

type DBSample struct {
	OffsetSec       float64 `json:"offsetSec"`
	OutboxBacklog   int64   `json:"outboxBacklog"`
	OutboxOldestSec float64 `json:"outboxOldestSec"`
	PendingRefs     int64   `json:"pendingReferences"`
	LockWaits       int64   `json:"lockWaits"`
	Connections     int64   `json:"connections"`
	QueueDepth      int64   `json:"queueDepth"`
}

type Run struct {
	RunID    string         `json:"runId"`
	Scenario string         `json:"scenario"`
	Command  []string       `json:"command"`
	Params   map[string]any `json:"params"`
	Targets  []string       `json:"targets"`

	StartedAt   time.Time `json:"startedAt"`
	WarmupEnds  time.Time `json:"warmupEndsAt"`
	LoadEndedAt time.Time `json:"loadEndedAt"`
	FinishedAt  time.Time `json:"finishedAt"`

	TasksScheduled int64   `json:"tasksScheduled"`
	TasksDropped   int64   `json:"tasksDropped"`
	TasksAbandoned int64   `json:"tasksAbandoned"`
	OpsJournaled   int64   `json:"opsJournaled"`
	OpsMeasured    int64   `json:"opsMeasured"`
	MeasuredSec    float64 `json:"measuredSeconds"`

	DispatchLag *hist.Histogram `json:"dispatchLag"`
	Series      []hist.Series   `json:"series"`
	Resolution  *hist.Histogram `json:"reversalResolution"`
	Steps       []Step          `json:"steps,omitempty"`
	StepSec     float64         `json:"stepSeconds,omitempty"`

	TokenFetches map[string]int64 `json:"tokenFetches"`
	HTTPRetries  int64            `json:"httpRetries"`
	HTTP503      int64            `json:"http503Retried"`
	SQSBatches   int64            `json:"sqsBatches"`
	SQSRetries   int64            `json:"sqsRetries"`
	DLQBefore    int64            `json:"dlqBefore"`
	DBSamples    []DBSample       `json:"dbSamples"`
	// Events consumed from the audit queue during the load, and their
	// delivery latency (event occurredAt to receipt).
	AuditDrained  int64           `json:"auditDrained"`
	EventDelivery *hist.Histogram `json:"eventDelivery"`

	Valid          bool     `json:"valid"`
	InvalidReasons []string `json:"invalidReasons"`
}

type Check struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Violations  int      `json:"violations"`
	Samples     []string `json:"samples,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type Outbox struct {
	Events       int64   `json:"events"`
	Unpublished  int64   `json:"unpublished"`
	P50Sec       float64 `json:"p50Sec"`
	P95Sec       float64 `json:"p95Sec"`
	P99Sec       float64 `json:"p99Sec"`
	MaxSec       float64 `json:"maxSec"`
	DrainSec     float64 `json:"drainSec"`
	Drained      bool    `json:"drained"`
	DrainDetails string  `json:"drainDetails,omitempty"`
	// Rates while the load ran, and while draining after it stopped.
	ProducedPerSec   float64 `json:"producedPerSec"`
	PublishedPerSec  float64 `json:"publishedPerSec"`
	BacklogAtStop    int64   `json:"backlogAtLoadStop"`
	QueueAtStop      int64   `json:"queueDepthAtLoadStop"`
	DrainPerSec      float64 `json:"outboxDrainPerSec"`
	QueueDrainPerSec float64 `json:"queueDrainPerSec"`
}

type APISample struct {
	Sampled  int      `json:"sampled"`
	Failed   int      `json:"failed"`
	Failures []string `json:"failures,omitempty"`
}

type Verify struct {
	Pass           bool                `json:"pass"`
	VerifiedAt     time.Time           `json:"verifiedAt"`
	Checks         []Check             `json:"checks"`
	Cross          journal.CrossResult `json:"cross"`
	SQSEndToEnd    *hist.Histogram     `json:"sqsEndToEnd"`
	Outbox         Outbox              `json:"outbox"`
	Reconciliation APISample           `json:"reconciliation"`
	Lookups        APISample           `json:"lookups"`
	DLQAfter       int64               `json:"dlqAfter"`
	AuditDrained   int64               `json:"auditDrained"`
	EventDelivery  *hist.Histogram     `json:"eventDelivery"`
	DLQDelta       int64               `json:"dlqDelta"`
	FailReasons    []string            `json:"failReasons"`
}

func Write(dir, name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), b, 0o644)
}

func Read(dir, name string, v any) error {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
