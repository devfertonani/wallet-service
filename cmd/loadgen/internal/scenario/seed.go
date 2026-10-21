package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/auth"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/hist"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/result"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/target"
	"github.com/google/uuid"
)

// SeedSpec is how many wallets to open, and with which balances.
type SeedSpec struct {
	Wallets      int
	OpeningMinor int64
	Hot          int
	HotOpenMinor int64
	Currency     string
	Concurrency  int
}

// Seed opens fresh wallets through POST /wallets, the real opening path
// (OPENING transaction, ledger entry and outbox events in one commit). Each
// run uses new players, so the journal oracle covers exactly its wallets and
// runs do not need a database reset in between.
func Seed(ctx context.Context, h *target.HTTP, tok *auth.Source, spec SeedSpec, logf func(string, ...any)) ([]journal.Wallet, result.Seed, error) {
	type job struct {
		hot     bool
		opening int64
	}
	jobs := make(chan job)
	var mu sync.Mutex
	var wallets []journal.Wallet
	var failures []string
	lat := hist.New()
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < max(spec.Concurrency, 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				player := uuid.NewString()
				body, _ := json.Marshal(target.OpenWalletRequest{PlayerID: player, InitialBalance: target.MoneyOf(j.opening, spec.Currency)})
				t0 := time.Now()
				res := h.Do(ctx, target.Request{Method: http.MethodPost, Path: "/wallets", Token: tok, Body: body, Pin: -1})
				d := time.Since(t0)
				var wr target.WalletResponse
				_ = json.Unmarshal(res.Body, &wr)
				mu.Lock()
				lat.Record(d)
				if res.Status == http.StatusCreated && wr.ID != "" {
					wallets = append(wallets, journal.Wallet{ID: wr.ID, PlayerID: player, Currency: spec.Currency, OpeningMinor: j.opening, Hot: j.hot})
				} else {
					msg := fmt.Sprintf("HTTP %d %.200s", res.Status, res.Body)
					if res.Err != nil {
						msg = res.Err.Error()
					}
					failures = append(failures, msg)
				}
				mu.Unlock()
			}
		}()
	}
	total := spec.Wallets + spec.Hot
	lastLog := time.Now()
	for i := 0; i < total; i++ {
		j := job{opening: spec.OpeningMinor}
		if i < spec.Hot {
			j = job{hot: true, opening: spec.HotOpenMinor}
		}
		select {
		case jobs <- j:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		if logf != nil && time.Since(lastLog) > 5*time.Second {
			logf("seed: %d/%d wallets", i, total)
			lastLog = time.Now()
		}
	}
	close(jobs)
	wg.Wait()

	secs := time.Since(start).Seconds()
	st := result.Seed{Wallets: len(wallets), Failed: len(failures), Seconds: secs, Concurrency: spec.Concurrency, Latency: lat}
	for _, w := range wallets {
		if w.Hot {
			st.Hot++
		}
	}
	if secs > 0 {
		st.PerSecond = float64(len(wallets)) / secs
	}
	if len(failures) > 0 {
		return wallets, st, fmt.Errorf("%d of %d wallets failed to open; first: %s", len(failures), total, failures[0])
	}
	return wallets, st, ctx.Err()
}
