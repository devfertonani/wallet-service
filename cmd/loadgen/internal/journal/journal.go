// Package journal records one line per operation the generator sent and
// what it observed. After the run, the journal is the independent oracle the
// database is checked against: expected balances are recomputed from it
// alone, and every definitive answer must match what was persisted.
package journal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	ChannelHTTP = "http"
	ChannelSQS  = "sqs"

	// RoleCopy marks an extra copy of an operation (duplicates scenario).
	RoleCopy = "copy"
	// RoleProbe marks a request that must be refused (409); it never counts
	// towards balances.
	RoleProbe = "probe"

	// StatusSent is the outcome of an SQS send acknowledged by the broker.
	StatusSent = "SENT"
	// StatusError is a final transport error or a final 5xx: unknown outcome.
	StatusError = "ERROR"
)

// Op is one request (HTTP call or SQS message) as seen by the generator.
type Op struct {
	Seq       int64  `json:"seq"`
	Task      string `json:"task"`
	TaskID    int64  `json:"taskId"`
	Role      string `json:"role,omitempty"`
	Expect    string `json:"expect,omitempty"` // probes: "409:IDEMPOTENCY_KEY_REUSED"
	Channel   string `json:"channel"`
	Target    string `json:"target,omitempty"`
	Kind      string `json:"kind"`
	Provider  string `json:"providerId"`
	ExtID     string `json:"externalTransactionId"`
	IdemKey   string `json:"idempotencyKey"`
	MessageID string `json:"messageId,omitempty"`
	WalletID  string `json:"walletId"`
	PlayerID  string `json:"playerId"`
	RoundID   string `json:"roundId"`
	Amount    int64  `json:"amountMinor"`
	Currency  string `json:"currency"`
	RefExtID  string `json:"referenceExternalTransactionId,omitempty"`
	RefKind   string `json:"referenceKind,omitempty"`

	Scheduled time.Time `json:"scheduledAt"`
	Done      time.Time `json:"doneAt"`
	Warmup    bool      `json:"warmup,omitempty"`
	Step      int       `json:"step,omitempty"`

	Attempts      int    `json:"attempts"`
	HTTPStatus    int    `json:"httpStatus,omitempty"`
	Status        string `json:"status"`
	FailureCode   string `json:"failureCode,omitempty"`
	ErrorCode     string `json:"errorCode,omitempty"`
	TransactionID string `json:"transactionId,omitempty"`
	BalanceMinor  *int64 `json:"balanceMinor,omitempty"`
	Replay        bool   `json:"idempotentReplay,omitempty"`
	Err           string `json:"error,omitempty"`

	// Reversal-first: final state observed by polling after the reference
	// was sent, and how long it took to resolve.
	Resolved     string  `json:"resolvedStatus,omitempty"`
	ResolvedInMs float64 `json:"resolvedInMs,omitempty"`
}

// Wallet is one wallet seeded for the run.
type Wallet struct {
	ID           string `json:"id"`
	PlayerID     string `json:"playerId"`
	Currency     string `json:"currency"`
	OpeningMinor int64  `json:"openingMinor"`
	Hot          bool   `json:"hot,omitempty"`
}

// Writer appends ops as NDJSON; safe for concurrent use.
type Writer struct {
	mu  sync.Mutex
	f   *os.File
	bw  *bufio.Writer
	enc *json.Encoder
	n   int64
}

func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<16)
	return &Writer{f: f, bw: bw, enc: json.NewEncoder(bw)}, nil
}

func (w *Writer) Write(op Op) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.n++
	return w.enc.Encode(op)
}

func (w *Writer) Count() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

func ReadOps(path string) ([]Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ops []Op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	line := 0
	for sc.Scan() {
		line++
		if len(sc.Bytes()) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		ops = append(ops, op)
	}
	return ops, sc.Err()
}

func WriteWallets(path string, ws []Wallet) error {
	b, err := json.MarshalIndent(ws, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func ReadWallets(path string) ([]Wallet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ws []Wallet
	return ws, json.Unmarshal(b, &ws)
}
