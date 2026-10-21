//go:build integration

package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/app"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/testsupport/awstest"
	"github.com/devfertonani/wallet-service/internal/testsupport/pgtest"
	"github.com/google/uuid"
	"go.uber.org/fx"
	"go.uber.org/goleak"
)

const issuer = "http://localhost:8081/realms/wallet"

func keycloakURL() string {
	if v := os.Getenv("TEST_OIDC_ISSUER_URL"); v != "" {
		return v
	}
	return issuer
}

// token obtains a real access token from Keycloak with client_credentials.
func token(t *testing.T, client string) string {
	t.Helper()
	resp, err := http.PostForm(keycloakURL()+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		t.Fatalf("keycloak (is `docker compose up -d keycloak` running?): %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d, %v", client, resp.StatusCode, err)
	}
	return body.AccessToken
}

type instance struct {
	t    *testing.T
	base string
	app  *fx.App
	db   *pgtest.DB
	aws  *awstest.Env
	cfg  config.Config
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// start builds the real application graph, against a throwaway database,
// isolated queues and the real Keycloak, and starts it.
func start(t *testing.T) *instance {
	t.Helper()
	in := prepare(t)
	in.launch()
	return in
}

// prepare creates the dependencies and the graph without starting it.
func prepare(t *testing.T) *instance {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "000000000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("LOG_LEVEL", "error")
	db, aws := pgtest.New(t), awstest.New(t)
	cfg, err := config.Read()
	if err != nil {
		t.Fatal(err)
	}
	cfg.InstanceID = "test-" + uuid.NewString()[:8]
	cfg.HTTPAddr = freeAddr(t)
	cfg.DatabaseURL = db.AppURL
	cfg.AWSRegion, cfg.AWSEndpointURL = aws.Config.AWSRegion, aws.Config.AWSEndpointURL
	cfg.WagerQueueName, cfg.WagerDLQName, cfg.EventsTopic = aws.Config.WagerQueueName, aws.Config.WagerDLQName, aws.Config.EventsTopic
	cfg.OIDCIssuerURL = keycloakURL()
	cfg.OIDCJWKSURL = keycloakURL() + "/protocol/openid-connect/certs"
	cfg.OIDCAudience = "wallet-api"
	cfg.OIDCClockSkew = 0
	cfg.SQSWaitTime = time.Second
	cfg.PendingBaseDelay, cfg.PendingMaxDelay = 100*time.Millisecond, 500*time.Millisecond

	in := &instance{t: t, base: "http://" + cfg.HTTPAddr, db: db, aws: aws, cfg: cfg}
	t.Cleanup(func() { in.stop() })
	return in
}

func (in *instance) launch() {
	in.t.Helper()
	a := fx.New(app.Options(fx.Replace(in.cfg), fx.NopLogger), fx.StopTimeout(30*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		in.t.Fatalf("start: %v", err)
	}
	in.app = a
}

func (in *instance) stop() {
	if in.app == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := in.app.Stop(ctx); err != nil {
		in.t.Errorf("stop: %v", err)
	}
	in.app = nil
}

var client = &http.Client{Timeout: 10 * time.Second}

type response struct {
	status int
	body   map[string]any
}

func (in *instance) do(method, path, tok string, body any, headers ...string) response {
	in.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		if s, ok := body.(string); ok {
			b = []byte(s)
		}
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, in.base+path, rd)
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := client.Do(req)
	if err != nil {
		in.t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return response{resp.StatusCode, m}
}

func errCode(r response) string {
	e, _ := r.body["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func (in *instance) count(q string, args ...any) int {
	in.t.Helper()
	var n int
	if err := in.db.Owner.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		in.t.Fatal(err)
	}
	return n
}

func (in *instance) openWallet(tok, amount string) (walletID, playerID string) {
	in.t.Helper()
	playerID = uuid.NewString()
	r := in.do("POST", "/wallets", tok, map[string]any{"playerId": playerID, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}})
	if r.status != http.StatusCreated {
		in.t.Fatalf("open wallet: %d %v", r.status, r.body)
	}
	return r.body["id"].(string), playerID
}

func wagerBody(provider, ext, walletID, playerID, kind, amount string) map[string]any {
	return map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": playerID, "walletId": walletID,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"},
	}
}

// TestApplicationLifecycle starts the whole graph, exercises HTTP, SQS and the
// outbox through it, stops it, and checks that every worker goroutine ended.
func TestApplicationLifecycle(t *testing.T) {
	in := prepare(t)
	ignore := goleak.IgnoreCurrent()
	in.launch()
	internal, provider := token(t, "wallet-internal"), token(t, "provider-a")

	if r := in.do("GET", "/health/ready", "", nil); r.status != 200 {
		t.Fatalf("ready = %d %v", r.status, r.body)
	}
	walletID, playerID := in.openWallet(internal, "100.00")
	r := in.do("POST", "/wagering/transactions", provider, wagerBody("provider-a", "bet-1", walletID, playerID, "BET", "25.00"), "Idempotency-Key", "provider-a:bet-1")
	if r.status != 200 || r.body["status"] != "PROCESSED" {
		t.Fatalf("bet: %d %v", r.status, r.body)
	}
	// The same operation through SQS is a replay.
	in.aws.Send(t, map[string]any{"messageId": "m-1", "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339),
		"data": func() map[string]any {
			d := wagerBody("provider-a", "bet-1", walletID, playerID, "BET", "25.00")
			d["idempotencyKey"] = "provider-a:bet-1"
			return d
		}()}, walletID, "m-1")
	deadline := time.Now().Add(15 * time.Second)
	for in.count(`SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL`) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("SQS message not consumed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for in.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("outbox not drained")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := in.count(`SELECT count(*) FROM wallet_ledger_entries WHERE direction = 'DEBIT'`); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if got := len(in.aws.Drain(t, in.aws.EventsQueueURL, 2*time.Second)); got != 4 {
		t.Fatalf("published events = %d, want 4 (opening x2, bet x2)", got)
	}
	metrics := in.do("GET", "/metrics", "", nil)
	if metrics.status != 200 {
		t.Fatalf("metrics = %d", metrics.status)
	}

	in.stop()
	if _, err := client.Get(in.base + "/health/live"); err == nil {
		t.Fatal("HTTP still accepting after stop")
	}
	client.CloseIdleConnections()
	// Every worker, the pool and the HTTP server are gone. The AWS SDK's
	// idle keep-alive connections are not ours to close.
	goleak.VerifyNone(t, ignore,
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

func TestAuthentication(t *testing.T) {
	in := start(t)
	path := "/wallets/" + uuid.NewString()

	if r := in.do("GET", path, "", nil); r.status != 401 || errCode(r) != "UNAUTHENTICATED" {
		t.Fatalf("missing token: %d %v", r.status, r.body)
	}
	if r := in.do("GET", path, "not-a-jwt", nil); r.status != 401 {
		t.Fatalf("garbage token: %d", r.status)
	}
	good := token(t, "wallet-internal")
	parts := strings.Split(good, ".")
	tampered := parts[0] + "." + parts[1] + "x." + parts[2]
	if r := in.do("GET", path, tampered, nil); r.status != 401 {
		t.Fatalf("tampered token: %d", r.status)
	}
	if r := in.do("GET", path, parts[0]+"."+parts[1]+".", nil); r.status != 401 {
		t.Fatalf("unsigned token: %d", r.status)
	}
	short := token(t, "provider-a-short-lived") // 2 s lifespan in the realm
	time.Sleep(3 * time.Second)
	if r := in.do("GET", "/providers/provider-a/wagering/transactions/x", short, nil); r.status != 401 {
		t.Fatalf("expired token: %d", r.status)
	}
	if r := in.do("GET", path, token(t, "no-role-client"), nil); r.status != 403 {
		t.Fatalf("token without roles: %d", r.status)
	}
	if r := in.do("GET", path, good, nil); r.status != 404 {
		t.Fatalf("valid token on a missing wallet: %d", r.status)
	}
}

func TestAuthorizationAndProviderIsolation(t *testing.T) {
	in := start(t)
	internal, provA, provB := token(t, "wallet-internal"), token(t, "provider-a"), token(t, "provider-b")
	walletID, playerID := in.openWallet(internal, "100.00")

	a := in.do("POST", "/wagering/transactions", provA, wagerBody("provider-a", "bet-1", walletID, playerID, "BET", "10.00"), "Idempotency-Key", "shared-key")
	if a.status != 200 {
		t.Fatalf("provider-a bet: %d %v", a.status, a.body)
	}
	txID := a.body["transactionId"].(string)
	before := in.count(`SELECT count(*) FROM wager_transactions`)

	// Wallet operations are internal only.
	for _, c := range []struct{ method, path string }{
		{"POST", "/wallets"}, {"GET", "/wallets/" + walletID}, {"GET", "/wallets/" + walletID + "/ledger"},
		{"POST", "/wallets/" + walletID + "/reconciliation"},
	} {
		body := any(nil)
		if c.method == "POST" {
			body = map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}
		}
		if r := in.do(c.method, c.path, provA, body); r.status != 403 {
			t.Errorf("provider on %s %s: %d", c.method, c.path, r.status)
		}
	}
	// The internal service does not submit wagers in the name of a provider.
	if r := in.do("POST", "/wagering/transactions", internal, wagerBody("provider-a", "x", walletID, playerID, "BET", "1.00"), "Idempotency-Key", "k"); r.status != 403 {
		t.Errorf("internal submitting a wager: %d", r.status)
	}
	// provider-b cannot speak for provider-a, nor read its transactions.
	if r := in.do("POST", "/wagering/transactions", provB, wagerBody("provider-a", "bet-2", walletID, playerID, "BET", "1.00"), "Idempotency-Key", "k2"); r.status != 403 {
		t.Errorf("provider-b posing as provider-a: %d", r.status)
	}
	if r := in.do("GET", "/wagering/transactions/"+txID, provB, nil); r.status != 404 {
		t.Errorf("provider-b reading provider-a's transaction by id: %d", r.status)
	}
	if r := in.do("GET", "/providers/provider-a/wagering/transactions/bet-1", provB, nil); r.status != 403 {
		t.Errorf("provider-b reading provider-a's transaction by external id: %d", r.status)
	}
	if after := in.count(`SELECT count(*) FROM wager_transactions`); after != before {
		t.Fatalf("unauthorized calls changed financial data: %d -> %d", before, after)
	}

	// Replays are scoped by provider: provider-b reusing the same key and the
	// same external id is its own operation, never a replay of provider-a's.
	b := in.do("POST", "/wagering/transactions", provB, wagerBody("provider-b", "bet-1", walletID, playerID, "BET", "10.00"), "Idempotency-Key", "shared-key")
	if b.status != 200 || b.body["idempotentReplay"] != false || b.body["transactionId"] == txID {
		t.Fatalf("provider-b with provider-a's key: %d %v", b.status, b.body)
	}
	// Each provider reads its own; the internal service reads everything.
	if r := in.do("GET", "/providers/provider-a/wagering/transactions/bet-1", provA, nil); r.status != 200 || r.body["transactionId"] != txID {
		t.Errorf("provider-a reading its own: %d %v", r.status, r.body)
	}
	if r := in.do("GET", "/wagering/transactions/"+txID, internal, nil); r.status != 200 {
		t.Errorf("internal reading: %d", r.status)
	}
}

func TestHTTPContract(t *testing.T) {
	in := start(t)
	internal, prov := token(t, "wallet-internal"), token(t, "provider-a")
	walletID, playerID := in.openWallet(internal, "100.00")
	submit := func(body any, key string) response {
		return in.do("POST", "/wagering/transactions", prov, body, "Idempotency-Key", key)
	}

	if r := submit(wagerBody("provider-a", "a", walletID, playerID, "BET", "1.00"), ""); r.status != 400 || errCode(r) != "INVALID_REQUEST" {
		t.Errorf("missing Idempotency-Key: %d %v", r.status, r.body)
	}
	numeric := `{"providerId":"provider-a","externalTransactionId":"n","playerId":"` + playerID + `","walletId":"` + walletID +
		`","roundId":"r","gameId":"g","kind":"BET","money":{"amount":25.00,"currency":"BRL"}}`
	if r := submit(numeric, "n"); r.status != 400 || errCode(r) != "INVALID_AMOUNT" {
		t.Errorf("numeric amount: %d %v", r.status, r.body)
	}
	for _, amount := range []string{"25", "25.5", "25.005", "1e2", "NaN", "-1.00", "0.00"} {
		if r := submit(wagerBody("provider-a", "amt-"+amount, walletID, playerID, "BET", amount), "amt-"+amount); r.status != 400 || errCode(r) != "INVALID_AMOUNT" {
			t.Errorf("amount %q: %d %v", amount, r.status, r.body)
		}
	}
	if r := submit(wagerBody("provider-a", "o", walletID, playerID, "OPENING", "1.00"), "o"); r.status != 400 || errCode(r) != "UNSUPPORTED_KIND" {
		t.Errorf("OPENING: %d %v", r.status, r.body)
	}
	if r := submit(wagerBody("provider-a", "rf", walletID, playerID, "REFUND", "1.00"), "rf"); r.status != 400 || errCode(r) != "MISSING_REFERENCE" {
		t.Errorf("REFUND without reference: %d %v", r.status, r.body)
	}
	extra := wagerBody("provider-a", "x", walletID, playerID, "BET", "1.00")
	extra["unexpected"] = true
	if r := submit(extra, "x"); r.status != 400 {
		t.Errorf("unknown field: %d", r.status)
	}
	if r := submit(wagerBody("provider-a", "nw", uuid.NewString(), playerID, "BET", "1.00"), "nw"); r.status != 404 || errCode(r) != "WALLET_NOT_FOUND" {
		t.Errorf("missing wallet: %d %v", r.status, r.body)
	}
	if r := submit(wagerBody("provider-a", "big", walletID, playerID, "BET", "100.01"), "big"); r.status != 422 || r.body["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("insufficient funds: %d %v", r.status, r.body)
	}
	if r := submit(wagerBody("provider-a", "big", walletID, playerID, "BET", "100.01"), "big"); r.status != 422 || r.body["idempotentReplay"] != true {
		t.Errorf("replay of a rejection: %d %v", r.status, r.body)
	}
	if r := in.do("POST", "/wallets", internal, map[string]any{"playerId": playerID, "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}); r.status != 409 || errCode(r) != "WALLET_ALREADY_EXISTS" {
		t.Errorf("second wallet: %d %v", r.status, r.body)
	}

	// Ledger pagination walks every entry once, in order.
	for i := range 4 {
		submit(wagerBody("provider-a", fmt.Sprintf("p-%d", i), walletID, playerID, "BET", "1.00"), fmt.Sprintf("p-%d", i))
	}
	seen, cursor := 0, ""
	var last float64
	for page := 0; page < 10; page++ {
		path := "/wallets/" + walletID + "/ledger?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := in.do("GET", path, internal, nil)
		if r.status != 200 {
			t.Fatalf("ledger: %d %v", r.status, r.body)
		}
		for _, it := range r.body["items"].([]any) {
			v := it.(map[string]any)["walletVersion"].(float64)
			if v <= last {
				t.Fatalf("ledger not ordered: %v after %v", v, last)
			}
			last = v
			seen++
		}
		next, _ := r.body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if seen != 5 {
		t.Fatalf("ledger entries seen = %d, want 5", seen)
	}
	if r := in.do("GET", "/wallets/"+walletID+"/ledger?cursor=garbage", internal, nil); r.status != 400 {
		t.Errorf("invalid cursor: %d", r.status)
	}
	r := in.do("POST", "/wallets/"+walletID+"/reconciliation", internal, nil)
	if r.status != 200 || r.body["consistent"] != true || r.body["checkedEntries"].(float64) != 5 {
		t.Fatalf("reconciliation: %d %v", r.status, r.body)
	}
	if d := r.body["difference"].(map[string]any)["amount"]; d != "0.00" {
		t.Fatalf("difference = %v", d)
	}
}
