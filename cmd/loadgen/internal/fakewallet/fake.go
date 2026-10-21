// Package fakewallet is an in-memory stand-in for the wallet HTTP API and
// the token endpoint, for the generator's own end-to-end test. It follows
// the public contract (idempotency, 409 codes, 202 PENDING_REFERENCE, 422
// rejections, 503 with Retry-After) closely enough to drive every HTTP path
// of the generator; it is not a model of the real service's internals.
package fakewallet

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/journal"
	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/target"
	"github.com/google/uuid"
)

type tx struct {
	id, provider, ext, key, hash, wallet, kind, status, failure, refExt string
	amount, resultBalance                                               int64
	created                                                             time.Time
}

type wallet struct {
	id, player       string
	balance, version int64
	ledger           int64 // sum of entries, for reconciliation
	entries          int64
}

type Server struct {
	// Fail503 is the probability of answering 503 before doing anything.
	Fail503 float64

	mu       sync.Mutex
	wallets  map[string]*wallet
	byKey    map[[2]string]*tx
	byExt    map[[2]string]*tx
	pending  []*tx
	tokens   map[string]string // token -> client
	nTokens  int
	TokenHit int
}

func New() *Server {
	return &Server{wallets: map[string]*wallet{}, byKey: map[[2]string]*tx{}, byExt: map[[2]string]*tx{}, tokens: map[string]string{}}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", s.token)
	mux.HandleFunc("POST /wallets", s.auth("wallet-internal", s.openWallet))
	mux.HandleFunc("POST /wallets/{id}/reconciliation", s.auth("wallet-internal", s.reconcile))
	mux.HandleFunc("POST /wagering/transactions", s.auth("provider", s.wager))
	mux.HandleFunc("GET /providers/{p}/wagering/transactions/{ext}", s.auth("provider", s.lookup))
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": code}})
}

func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	id := r.Form.Get("client_id")
	if r.Form.Get("client_secret") != id+"-secret" {
		apiErr(w, 401, "invalid_client")
		return
	}
	s.mu.Lock()
	s.nTokens++
	s.TokenHit++
	tok := fmt.Sprintf("fake-%s-%d", id, s.nTokens)
	s.tokens[tok] = id
	s.mu.Unlock()
	writeJSON(w, 200, map[string]any{"access_token": tok, "expires_in": 300, "token_type": "Bearer"})
}

func (s *Server) auth(role string, next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		client, ok := s.tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		s.mu.Unlock()
		if !ok {
			apiErr(w, 401, "UNAUTHORIZED")
			return
		}
		if (role == "provider") != strings.HasPrefix(client, "provider-") {
			apiErr(w, 403, "FORBIDDEN")
			return
		}
		if r.Method == http.MethodPost && s.Fail503 > 0 && rand.Float64() < s.Fail503 {
			w.Header().Set("Retry-After", "1")
			apiErr(w, 503, "UNAVAILABLE")
			return
		}
		next(w, r, client)
	}
}

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request, _ string) {
	var req target.OpenWalletRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		apiErr(w, 400, "INVALID_REQUEST")
		return
	}
	amount, err := target.ParseMinor(req.InitialBalance.Amount)
	if err != nil {
		apiErr(w, 400, "INVALID_AMOUNT")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, x := range s.wallets {
		if x.player == req.PlayerID {
			apiErr(w, 409, "WALLET_ALREADY_EXISTS")
			return
		}
	}
	wl := &wallet{id: uuid.NewString(), player: req.PlayerID, balance: amount, version: 1, ledger: amount}
	if amount > 0 {
		wl.entries = 1
	}
	s.wallets[wl.id] = wl
	writeJSON(w, 201, target.WalletResponse{ID: wl.id, PlayerID: wl.player, Balance: target.MoneyOf(amount, "BRL"), Version: 1})
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wl := s.wallets[r.PathValue("id")]
	if wl == nil {
		apiErr(w, 404, "WALLET_NOT_FOUND")
		return
	}
	writeJSON(w, 200, target.ReconciliationResponse{WalletID: wl.id, StoredBalance: target.MoneyOf(wl.balance, "BRL"),
		CalculatedBalance: target.MoneyOf(wl.ledger, "BRL"), Difference: target.MoneyOf(wl.balance-wl.ledger, "BRL"),
		Consistent: wl.balance == wl.ledger, CheckedEntries: wl.entries})
}

func (s *Server) lookup(w http.ResponseWriter, r *http.Request, client string) {
	if r.PathValue("p") != client {
		apiErr(w, 403, "FORBIDDEN")
		return
	}
	s.mu.Lock()
	t := s.byExt[[2]string{client, r.PathValue("ext")}]
	s.mu.Unlock()
	if t == nil {
		apiErr(w, 404, "NOT_FOUND")
		return
	}
	writeJSON(w, 200, map[string]any{"transactionId": t.id, "status": t.status, "failureCode": t.failure})
}

func (s *Server) respond(w http.ResponseWriter, t *tx, replay bool) {
	code := map[string]int{"PROCESSED": 200, "PENDING_REFERENCE": 202, "REJECTED": 422}[t.status]
	body := map[string]any{"transactionId": t.id, "status": t.status, "idempotentReplay": replay}
	if t.status == "PROCESSED" {
		body["balance"] = target.MoneyOf(t.resultBalance, "BRL")
	}
	if t.failure != "" {
		body["failureCode"] = t.failure
	}
	writeJSON(w, code, body)
}

func (s *Server) wager(w http.ResponseWriter, r *http.Request, client string) {
	key := r.Header.Get("Idempotency-Key")
	var req target.WagerRequest
	if key == "" || json.NewDecoder(r.Body).Decode(&req) != nil {
		apiErr(w, 400, "INVALID_REQUEST")
		return
	}
	if req.ProviderID != client {
		apiErr(w, 403, "FORBIDDEN")
		return
	}
	amount, err := target.ParseMinor(req.Money.Amount)
	if err != nil {
		apiErr(w, 400, "INVALID_AMOUNT")
		return
	}
	hash := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s", req.ExternalTransactionID, req.PlayerID, req.WalletID, req.RoundID,
		req.GameID, req.Kind, req.Money.Amount, req.ReferenceExternalTransactionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.byKey[[2]string{client, key}]; t != nil {
		if t.hash != hash {
			apiErr(w, 409, "IDEMPOTENCY_KEY_REUSED")
			return
		}
		s.respond(w, t, true)
		return
	}
	if s.byExt[[2]string{client, req.ExternalTransactionID}] != nil {
		apiErr(w, 409, "DUPLICATE_EXTERNAL_TRANSACTION")
		return
	}
	wl := s.wallets[req.WalletID]
	if wl == nil {
		apiErr(w, 404, "WALLET_NOT_FOUND")
		return
	}
	t := &tx{id: uuid.NewString(), provider: client, ext: req.ExternalTransactionID, key: key, hash: hash, wallet: wl.id,
		kind: req.Kind, amount: amount, refExt: req.ReferenceExternalTransactionID, created: time.Now()}
	s.byKey[[2]string{client, key}] = t
	s.byExt[[2]string{client, t.ext}] = t
	s.apply(t, wl)
	if t.kind == "BET" {
		s.resolvePending(client, t.ext)
	}
	s.respond(w, t, false)
}

func (s *Server) move(t *tx, wl *wallet, delta int64) {
	if delta == 0 {
		t.status, t.resultBalance = "PROCESSED", wl.balance
		return
	}
	if wl.balance+delta < 0 {
		t.status, t.failure = "REJECTED", "INSUFFICIENT_FUNDS"
		if t.kind == "ROLLBACK" {
			t.failure = "REVERSAL_INSUFFICIENT_FUNDS"
		}
		return
	}
	wl.balance += delta
	wl.ledger += delta
	wl.version++
	wl.entries++
	t.status, t.resultBalance = "PROCESSED", wl.balance
}

func (s *Server) apply(t *tx, wl *wallet) {
	switch t.kind {
	case "BET":
		s.move(t, wl, -t.amount)
	case "WIN":
		s.move(t, wl, t.amount)
	case "LOSS":
		s.move(t, wl, 0)
	case "REFUND", "ROLLBACK":
		ref := s.byExt[[2]string{t.provider, t.refExt}]
		switch {
		case ref == nil:
			t.status = "PENDING_REFERENCE"
			s.pending = append(s.pending, t)
		case ref.status != "PROCESSED":
			t.status, t.failure = "REJECTED", "REFERENCE_NOT_PROCESSED"
		case ref.amount != t.amount:
			t.status, t.failure = "REJECTED", "AMOUNT_MISMATCH"
		case s.reversed(ref):
			t.status, t.failure = "REJECTED", "ALREADY_REVERSED"
		default:
			s.move(t, wl, journal.Effect(t.kind, ref.kind, t.amount))
		}
	}
}

func (s *Server) reversed(ref *tx) bool {
	for _, o := range s.byExt {
		if (o.kind == "REFUND" || o.kind == "ROLLBACK") && o.status == "PROCESSED" && o.refExt == ref.ext && o.provider == ref.provider {
			return true
		}
	}
	return false
}

func (s *Server) resolvePending(provider, ext string) {
	keep := s.pending[:0]
	var ready []*tx
	for _, p := range s.pending {
		if p.provider == provider && p.refExt == ext {
			ready = append(ready, p)
		} else {
			keep = append(keep, p)
		}
	}
	s.pending = keep
	for _, p := range ready {
		s.apply(p, s.wallets[p.wallet])
	}
}

// State returns the persisted rows in the shape verify reads from Postgres.
func (s *Server) State() ([]journal.DBTx, []journal.DBWallet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var txs []journal.DBTx
	for _, t := range s.byExt {
		txs = append(txs, journal.DBTx{ID: t.id, Provider: t.provider, ExtID: t.ext, Kind: t.kind, Status: t.status,
			FailureCode: t.failure, Amount: t.amount, WalletID: t.wallet, CreatedAt: t.created})
	}
	var ws []journal.DBWallet
	for _, w := range s.wallets {
		ws = append(ws, journal.DBWallet{ID: w.id, Balance: w.balance, Version: w.version})
	}
	return txs, ws
}
