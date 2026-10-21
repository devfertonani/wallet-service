package journal

import (
	"fmt"
	"sort"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
)

// Effect is the signed balance change of an operation when PROCESSED.
// A ROLLBACK reverts the referenced operation, so its sign is the opposite of
// the reference's.
func Effect(kind, refKind string, amount int64) int64 {
	switch kind {
	case "BET":
		return -amount
	case "WIN", "REFUND", "OPENING":
		return amount
	case "ROLLBACK":
		return -Effect(refKind, "", amount)
	default: // LOSS and anything unknown
		return 0
	}
}

// Moves reports whether a PROCESSED operation writes a ledger entry.
func Moves(kind, refKind string, amount int64) bool { return Effect(kind, refKind, amount) != 0 }

// DBTx is a persisted wager transaction, as read by verify.
type DBTx struct {
	ID          string
	Provider    string
	ExtID       string
	Kind        string
	Status      string
	FailureCode string
	Amount      int64
	WalletID    string
	CreatedAt   time.Time
}

// DBWallet is a persisted wallet row.
type DBWallet struct {
	ID      string
	Balance int64
	Version int64
}

type Key struct{ Provider, ExtID string }

func (o Op) Key() Key { return Key{o.Provider, o.ExtID} }

// Violation is one broken expectation; any violation fails the run.
type Violation struct {
	Check  string `json:"check"`
	Detail string `json:"detail"`
}

// CrossStats are informative numbers from the cross check.
type CrossStats struct {
	LogicalOps        int            `json:"logicalOps"`
	Copies            int            `json:"copies"`
	Probes            int            `json:"probes"`
	ByFinalStatus     map[string]int `json:"byFinalStatus"`
	UnknownNotApplied int            `json:"unknownNotApplied"`
	UnknownApplied    int            `json:"unknownApplied"`
	StillPending      int            `json:"stillPendingReference"`
	Replays           int            `json:"httpReplays"`
	WalletsChecked    int            `json:"walletsChecked"`
}

// CrossResult is the outcome of comparing the journal with the database.
type CrossResult struct {
	Violations []Violation `json:"violations"`
	Stats      CrossStats  `json:"stats"`
	// SQSEndToEnd is scheduled send -> row committed, for SQS operations.
	SQSEndToEnd *hist.Histogram `json:"-"`
}

const maxPerCheck = 20

type collector struct {
	out    []Violation
	counts map[string]int
}

func (c *collector) add(check, format string, args ...any) {
	c.counts[check]++
	if c.counts[check] <= maxPerCheck {
		c.out = append(c.out, Violation{Check: check, Detail: fmt.Sprintf(format, args...)})
	} else if c.counts[check] == maxPerCheck+1 {
		c.out = append(c.out, Violation{Check: check, Detail: "further violations of this check omitted"})
	}
}

// CrossCheck compares the journal (what the generator sent and saw) with the
// rows persisted for the run's wallets.
func CrossCheck(wallets []Wallet, ops []Op, txs []DBTx, dbWallets []DBWallet) CrossResult {
	c := &collector{counts: map[string]int{}}
	res := CrossResult{SQSEndToEnd: hist.New(), Stats: CrossStats{ByFinalStatus: map[string]int{}}}

	txByKey := make(map[Key]DBTx, len(txs))
	for _, t := range txs {
		k := Key{t.Provider, t.ExtID}
		if _, dup := txByKey[k]; dup {
			c.add("db_duplicate_operation", "%s/%s persisted more than once", t.Provider, t.ExtID)
		}
		txByKey[k] = t
	}

	type logical struct {
		rep    Op
		copies []Op
	}
	byKey := map[Key]*logical{}
	var order []Key
	for _, op := range ops {
		if op.Role == RoleProbe {
			res.Stats.Probes++
			checkProbe(c, op)
			continue
		}
		k := op.Key()
		l, ok := byKey[k]
		if !ok {
			l = &logical{rep: op}
			byKey[k] = l
			order = append(order, k)
		}
		l.copies = append(l.copies, op)
		if op.Role == RoleCopy {
			res.Stats.Copies++
		}
	}
	res.Stats.LogicalOps = len(byKey)

	walletSet := map[string]Wallet{}
	for _, w := range wallets {
		walletSet[w.ID] = w
	}
	balance := map[string]int64{}
	version := map[string]int64{}
	for _, w := range wallets {
		balance[w.ID] = w.OpeningMinor
		version[w.ID] = 1
	}

	for _, k := range order {
		l := byKey[k]
		rep := l.rep
		db, found := txByKey[k]
		if found {
			if db.Kind != rep.Kind || db.Amount != rep.Amount || db.WalletID != rep.WalletID {
				c.add("payload_mismatch", "%s: sent %s %d on %s, persisted %s %d on %s",
					rep.ExtID, rep.Kind, rep.Amount, rep.WalletID, db.Kind, db.Amount, db.WalletID)
			}
		}

		definitive := false // the generator saw an answer that fixes the outcome
		applied := false    // the generator saw evidence the op was recorded
		originals := 0
		var firstTx string
		var firstBal *int64
		for _, op := range l.copies {
			switch {
			case op.Channel == ChannelSQS && op.Status == StatusSent:
				applied = true
			case op.HTTPStatus == 200 || op.HTTPStatus == 422 || op.HTTPStatus == 202:
				applied = true
				if op.HTTPStatus != 202 {
					definitive = true
				}
				if op.HTTPStatus == 200 {
					if op.Replay {
						res.Stats.Replays++
					} else {
						originals++
					}
					if firstTx == "" {
						firstTx, firstBal = op.TransactionID, op.BalanceMinor
					} else if op.TransactionID != firstTx || !sameBalance(op.BalanceMinor, firstBal) {
						c.add("replay_mismatch", "%s: copies answered different results (%s vs %s)", op.ExtID, firstTx, op.TransactionID)
					}
				}
				if found {
					if op.TransactionID != "" && op.TransactionID != db.ID {
						c.add("transaction_id_mismatch", "%s: answered %s, persisted %s", op.ExtID, op.TransactionID, db.ID)
					}
					if op.HTTPStatus != 202 && (op.Status != db.Status || op.FailureCode != db.FailureCode) {
						c.add("status_mismatch", "%s: answered %s %s, persisted %s %s", op.ExtID, op.Status, op.FailureCode, db.Status, db.FailureCode)
					}
				}
			case op.Status == StatusError || op.HTTPStatus >= 500:
				// unknown outcome; resolved from the database below
			default:
				c.add("unexpected_http_status", "%s %s: HTTP %d %s", op.Kind, op.ExtID, op.HTTPStatus, op.ErrorCode)
			}
		}
		if originals > 1 {
			c.add("double_original", "%s: %d copies were processed as originals", rep.ExtID, originals)
		}
		if applied && !found {
			check := "journal_op_missing"
			if !definitive {
				check = "sqs_message_not_applied"
			}
			c.add(check, "%s %s on %s not persisted", rep.Kind, rep.ExtID, rep.WalletID)
		}
		if !applied {
			if found {
				res.Stats.UnknownApplied++
			} else {
				res.Stats.UnknownNotApplied++
			}
		}
		if !found {
			continue
		}
		res.Stats.ByFinalStatus[db.Status]++
		if db.Status == "PENDING_REFERENCE" {
			res.Stats.StillPending++
		}
		if rep.Channel == ChannelSQS && !rep.Scheduled.IsZero() && !rep.Warmup {
			res.SQSEndToEnd.Record(db.CreatedAt.Sub(rep.Scheduled))
		}
		if _, ours := walletSet[rep.WalletID]; ours && db.Status == "PROCESSED" {
			eff := Effect(rep.Kind, rep.RefKind, rep.Amount)
			balance[rep.WalletID] += eff
			if eff != 0 {
				version[rep.WalletID]++
			}
		}
	}

	for _, t := range txs {
		if _, ok := byKey[Key{t.Provider, t.ExtID}]; !ok {
			c.add("unexpected_transaction", "%s %s/%s on %s is not in the journal", t.Kind, t.Provider, t.ExtID, t.WalletID)
		}
	}

	dbw := map[string]DBWallet{}
	for _, w := range dbWallets {
		dbw[w.ID] = w
	}
	ids := make([]string, 0, len(wallets))
	for _, w := range wallets {
		ids = append(ids, w.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		got, ok := dbw[id]
		if !ok {
			c.add("wallet_missing", "seeded wallet %s not found", id)
			continue
		}
		res.Stats.WalletsChecked++
		if got.Balance != balance[id] {
			c.add("balance_vs_journal", "wallet %s: stored %d, journal expects %d", id, got.Balance, balance[id])
		}
		if got.Version != version[id] {
			c.add("version_vs_journal", "wallet %s: version %d, journal expects %d (lost or extra update)", id, got.Version, version[id])
		}
	}
	res.Violations = append([]Violation{}, c.out...)
	return res
}

func checkProbe(c *collector, op Op) {
	if op.Status == StatusError || op.HTTPStatus >= 500 {
		return // no answer; nothing to judge
	}
	want := op.Expect
	got := fmt.Sprintf("%d:%s", op.HTTPStatus, op.ErrorCode)
	if got != want {
		c.add("probe_unexpected", "%s on %s: want %s, got %s", op.Kind, op.ExtID, want, got)
	}
}

func sameBalance(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
