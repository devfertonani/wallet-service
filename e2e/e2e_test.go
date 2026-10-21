//go:build e2e

// Package e2e runs the required concurrency and recovery scenarios against
// the three independent instances started by docker compose (wallet-1,
// wallet-2, wallet-3), each with its own process, memory and connection pool.
//
//	docker compose up -d --build
//	go test -tags e2e -race -count=1 ./e2e/
//
// Tests that restart or kill containers need E2E_DOCKER=1.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/devfertonani/wallet-service/internal/testsupport/dotenv"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	_         = loadEnv()
	instances = strings.Split(env("E2E_INSTANCES", "http://localhost:8091,http://localhost:8092,http://localhost:8093"), ",")
	tokenURL  = env("E2E_TOKEN_URL", "http://localhost:8081/realms/wallet/protocol/openid-connect/token")
	queueURL  = env("E2E_QUEUE_URL", "http://localhost:4566/000000000000/wager-transactions.fifo")
	awsURL    = env("E2E_AWS_ENDPOINT_URL", "http://localhost:4566")
	dbURL     = env("E2E_DATABASE_URL", "postgres://wallet_owner:wallet_owner@localhost:"+dotenv.HostPort()+"/wallet?sslmode=disable")
)

// loadEnv reads the repository .env before the defaults above are computed.
func loadEnv() bool { dotenv.Load(); return true }

var httpClient = &http.Client{Timeout: 15 * time.Second}

type suite struct {
	t        *testing.T
	internal string
	provider string
	db       *pgxpool.Pool
	sqs      *sqs.Client
}

func setup(t *testing.T) *suite {
	t.Helper()
	s := &suite{t: t, internal: token(t, "wallet-internal"), provider: token(t, "provider-a")}
	var err error
	if s.db, err = pgxpool.New(context.Background(), dbURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.db.Close)
	for _, base := range instances {
		resp, err := httpClient.Get(base + "/health/ready")
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s not ready (is `docker compose up -d --build` running?): %v", base, err)
		}
		resp.Body.Close()
	}
	ac, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("000000000000", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	ac.BaseEndpoint = aws.String(awsURL)
	s.sqs = sqs.NewFromConfig(ac)
	return s
}

func token(t *testing.T, client string) string {
	t.Helper()
	resp, err := http.PostForm(tokenURL, url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil || b.AccessToken == "" {
		t.Fatalf("token %s: %v", client, err)
	}
	return b.AccessToken
}

func (s *suite) call(base, method, path, tok string, body any, key string) (int, map[string]any) {
	s.t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, base+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, map[string]any{"error": err.Error()}
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

type wallet struct{ id, player string }

func (s *suite) open(amount string) wallet {
	s.t.Helper()
	player := uuid.NewString()
	code, body := s.call(instances[0], "POST", "/wallets", s.internal, map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}}, "")
	if code != 201 {
		s.t.Fatalf("open: %d %v", code, body)
	}
	return wallet{body["id"].(string), player}
}

func op(w wallet, ext, kind, amount, ref string) map[string]any {
	m := map[string]any{"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.player, "walletId": w.id,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"}}
	if ref != "" {
		m["referenceExternalTransactionId"] = ref
	}
	return m
}

// submit retries 503 with the same key, as a provider would.
func (s *suite) submit(base string, body map[string]any) (int, map[string]any) {
	key := "provider-a:" + body["externalTransactionId"].(string)
	for attempt := 0; ; attempt++ {
		code, resp := s.call(base, "POST", "/wagering/transactions", s.provider, body, key)
		if (code == 503 || code == 0) && attempt < 20 {
			time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
			continue
		}
		return code, resp
	}
}

func (s *suite) sendSQS(w wallet, messageID string, body map[string]any) {
	s.t.Helper()
	data := map[string]any{}
	for k, v := range body {
		data[k] = v
	}
	data["idempotencyKey"] = "provider-a:" + body["externalTransactionId"].(string)
	b, _ := json.Marshal(map[string]any{"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339), "data": data})
	if _, err := s.sqs.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(queueURL),
		MessageBody: aws.String(string(b)), MessageGroupId: aws.String(w.id), MessageDeduplicationId: aws.String(messageID)}); err != nil {
		s.t.Fatal(err)
	}
}

func (s *suite) count(q string, args ...any) int {
	s.t.Helper()
	var n int
	if err := s.db.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		s.t.Fatal(err)
	}
	return n
}

func (s *suite) debits(w wallet) int {
	return s.count(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id)
}

// reconcile asks a random instance and fails on any divergence.
func (s *suite) reconcile(w wallet) string {
	s.t.Helper()
	code, body := s.call(instances[len(w.id)%len(instances)], "POST", "/wallets/"+w.id+"/reconciliation", s.internal, nil, "")
	if code != 200 || body["consistent"] != true {
		s.t.Fatalf("reconciliation of %s: %d %v", w.id, code, body)
	}
	return body["storedBalance"].(map[string]any)["amount"].(string)
}

func (s *suite) eventually(timeout time.Duration, what string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out: %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Required tests 1 and 4: the same bet 50 times in parallel, spread over the
// three instances, plus copies through SQS.
func TestSameBetFiftyTimesAcrossInstances(t *testing.T) {
	s := setup(t)
	w := s.open("100.00")
	bet := op(w, "bet-"+uuid.NewString(), "BET", "25.00", "")
	for i := range 10 {
		s.sendSQS(w, fmt.Sprintf("%s-copy-%d", bet["externalTransactionId"], i), bet)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	fresh, ids := 0, map[string]bool{}
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body := s.submit(instances[i%len(instances)], bet)
			mu.Lock()
			defer mu.Unlock()
			if code != 200 || body["status"] != "PROCESSED" {
				t.Errorf("copy %d: %d %v", i, code, body)
				return
			}
			if body["idempotentReplay"] == false {
				fresh++
			}
			ids[body["transactionId"].(string)] = true
		}()
	}
	wg.Wait()
	s.eventually(30*time.Second, "SQS copies consumed", func() bool {
		return s.count(`SELECT count(*) FROM inbox_messages WHERE message_id LIKE $1 AND completed_at IS NOT NULL`, bet["externalTransactionId"].(string)+"-copy-%") == 10
	})
	if fresh > 1 || len(ids) != 1 {
		t.Fatalf("fresh=%d distinct ids=%d", fresh, len(ids))
	}
	if n := s.debits(w); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if got := s.reconcile(w); got != "75.00" {
		t.Fatalf("balance = %s", got)
	}
}

// Required tests 2 and 4: two bets of 80.00 on 100.00, on different instances.
func TestTwoBetsOfEightyOnDifferentInstances(t *testing.T) {
	s := setup(t)
	for round := range 5 {
		w := s.open("100.00")
		bets := []map[string]any{op(w, "a-"+uuid.NewString(), "BET", "80.00", ""), op(w, "b-"+uuid.NewString(), "BET", "80.00", "")}
		codes := make([]int, 2)
		bodies := make([]map[string]any, 2)
		var wg sync.WaitGroup
		for i := range 2 {
			wg.Add(1)
			go func() { defer wg.Done(); codes[i], bodies[i] = s.submit(instances[(round+i)%len(instances)], bets[i]) }()
		}
		wg.Wait()
		ok, rejected := 0, 0
		for i := range 2 {
			switch {
			case codes[i] == 200:
				ok++
			case codes[i] == 422 && bodies[i]["failureCode"] == "INSUFFICIENT_FUNDS":
				rejected++
			}
		}
		if ok != 1 || rejected != 1 {
			t.Fatalf("round %d: %v %v", round, codes, bodies)
		}
		if n := s.debits(w); n != 1 {
			t.Fatalf("debits = %d", n)
		}
		if got := s.reconcile(w); got != "20.00" {
			t.Fatalf("balance = %s", got)
		}
		// Resends through every instance do not change the outcome.
		for i, b := range bets {
			for _, base := range instances {
				if code, _ := s.submit(base, b); code != codes[i] {
					t.Fatalf("resend changed %d -> %d", codes[i], code)
				}
			}
		}
		if got := s.reconcile(w); got != "20.00" {
			t.Fatalf("balance after resends = %s", got)
		}
	}
}

// Required tests 3 and 4: distinct wallets processed simultaneously by the
// three instances.
func TestDistinctWalletsAcrossInstances(t *testing.T) {
	s := setup(t)
	ws := make([]wallet, 30)
	for i := range ws {
		ws[i] = s.open("50.00")
	}
	var wg sync.WaitGroup
	start := time.Now()
	for i, w := range ws {
		for j := range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if code, body := s.submit(instances[(i+j)%len(instances)], op(w, fmt.Sprintf("p-%s-%d", uuid.NewString(), j), "BET", "10.00", "")); code != 200 {
					t.Errorf("wallet %d bet %d: %d %v", i, j, code, body)
				}
			}()
		}
	}
	wg.Wait()
	t.Logf("150 bets on 30 wallets over 3 instances in %s", time.Since(start))
	for _, w := range ws {
		if got := s.reconcile(w); got != "0.00" {
			t.Fatalf("wallet %s balance = %s", w.id, got)
		}
	}
}

func docker(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = ".."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func needDocker(t *testing.T) {
	if os.Getenv("E2E_DOCKER") != "1" {
		t.Skip("set E2E_DOCKER=1 to restart and kill containers")
	}
}

func (s *suite) waitReady() {
	s.eventually(90*time.Second, "instances ready", func() bool {
		for _, base := range instances {
			resp, err := httpClient.Get(base + "/health/ready")
			if err != nil {
				return false
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				return false
			}
		}
		return true
	})
}

// Required test 8: restart every instance; idempotency, pending references
// and financial consistency are preserved.
func TestRestartPreservesIdempotencyAndPendingReferences(t *testing.T) {
	needDocker(t)
	s := setup(t)
	w := s.open("100.00")
	bet := op(w, "bet-"+uuid.NewString(), "BET", "30.00", "")
	_, first := s.submit(instances[0], bet)
	late := "late-" + uuid.NewString()
	if code, body := s.submit(instances[1], op(w, "refund-"+uuid.NewString(), "REFUND", "10.00", late)); code != 202 {
		t.Fatalf("pending refund: %d %v", code, body)
	}

	docker(t, "restart", "wallet-1", "wallet-2", "wallet-3")
	s.waitReady()

	_, replay := s.submit(instances[2], bet)
	if replay["idempotentReplay"] != true || replay["transactionId"] != first["transactionId"] ||
		replay["balance"].(map[string]any)["amount"] != "70.00" {
		t.Fatalf("replay after restart: %v (first %v)", replay, first)
	}
	if code, _ := s.submit(instances[0], op(w, late, "BET", "10.00", "")); code != 200 {
		t.Fatalf("late bet: %d", code)
	}
	s.eventually(15*time.Second, "pending refund resolved after restart", func() bool {
		return s.count(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'REFUND' AND status = 'PROCESSED'`, w.id) == 1
	})
	if got := s.reconcile(w); got != "70.00" {
		t.Fatalf("balance = %s", got)
	}
}

// An instance is killed (SIGKILL) in the middle of an SQS burst: messages it
// held become visible again and are handled by the survivors or by itself
// after it comes back; nothing is lost or applied twice.
func TestKilledInstanceDuringSQSBurst(t *testing.T) {
	needDocker(t)
	s := setup(t)
	ws := make([]wallet, 10)
	for i := range ws {
		ws[i] = s.open("100.00")
	}
	prefix := "burst-" + uuid.NewString()[:8]
	for i := range 200 {
		w := ws[i%len(ws)]
		s.sendSQS(w, fmt.Sprintf("%s-%d", prefix, i), op(w, fmt.Sprintf("%s-%d", prefix, i), "BET", "1.00", ""))
	}
	time.Sleep(300 * time.Millisecond)
	docker(t, "kill", "-s", "SIGKILL", "wallet-1")
	time.Sleep(2 * time.Second)
	docker(t, "up", "-d", "wallet-1")
	s.waitReady()
	// Messages held by the killed process reappear after the 30 s visibility timeout.
	s.eventually(120*time.Second, "all 200 messages handled exactly once", func() bool {
		return s.count(`SELECT count(*) FROM inbox_messages WHERE message_id LIKE $1 AND completed_at IS NOT NULL`, prefix+"-%") == 200
	})
	for _, w := range ws {
		if n := s.debits(w); n != 20 {
			t.Fatalf("wallet %s debits = %d, want 20", w.id, n)
		}
		if got := s.reconcile(w); got != "80.00" {
			t.Fatalf("wallet %s balance = %s", w.id, got)
		}
	}
}

// Graceful stop (SIGTERM) under HTTP load: in-flight requests finish, and the
// operations that failed during the stop can be retried with the same key.
func TestGracefulStopUnderLoad(t *testing.T) {
	needDocker(t)
	s := setup(t)
	w := s.open("1000.00")
	var wg sync.WaitGroup
	results := make([]int, 60)
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i*20) * time.Millisecond)
			// Only wallet-1 is stopped; the provider fails over on errors.
			code, _ := s.submit(instances[0], op(w, fmt.Sprintf("g-%s-%d", w.id[:8], i), "BET", "1.00", ""))
			if code == 0 || code == 503 {
				code, _ = s.submit(instances[1], op(w, fmt.Sprintf("g-%s-%d", w.id[:8], i), "BET", "1.00", ""))
			}
			results[i] = code
		}()
	}
	time.Sleep(300 * time.Millisecond)
	docker(t, "stop", "-t", "40", "wallet-1")
	wg.Wait()
	docker(t, "up", "-d", "wallet-1")
	s.waitReady()
	for i, code := range results {
		if code != 200 {
			t.Fatalf("operation %d ended with %d", i, code)
		}
	}
	if n := s.debits(w); n != 60 {
		t.Fatalf("debits = %d, want 60", n)
	}
	if got := s.reconcile(w); got != "940.00" {
		t.Fatalf("balance = %s", got)
	}
}

// Senior review A2: with Postgres frozen, a request answers 503 within its
// deadline instead of hanging, and the retry with the same key, once the
// database is back, applies the operation exactly once.
func TestFrozenDatabaseAnswers503(t *testing.T) {
	needDocker(t)
	s := setup(t)
	w := s.open("10.00")
	bet := op(w, "frozen-"+uuid.NewString(), "BET", "1.00", "")
	key := "provider-a:" + bet["externalTransactionId"].(string)

	docker(t, "pause", "postgres")
	paused := true
	defer func() {
		if paused {
			docker(t, "unpause", "postgres")
		}
	}()
	start := time.Now()
	code, body := s.call(instances[0], "POST", "/wagering/transactions", s.provider, bet, key)
	elapsed := time.Since(start)
	docker(t, "unpause", "postgres")
	paused = false

	if code != 503 || elapsed > 13*time.Second {
		t.Fatalf("frozen database: %d %v after %s", code, body, elapsed)
	}
	if code, body := s.submit(instances[1], bet); code != 200 {
		t.Fatalf("retry after unpause: %d %v", code, body)
	}
	if n := s.debits(w); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if got := s.reconcile(w); got != "9.00" {
		t.Fatalf("balance = %s", got)
	}
}
