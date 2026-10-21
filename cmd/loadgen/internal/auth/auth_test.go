package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func tokenServer(t *testing.T, calls *atomic.Int64, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("client_secret") != "s3cret" {
			http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
			return
		}
		n := calls.Add(1)
		time.Sleep(delay)
		fmt.Fprintf(w, `{"access_token":"tok-%d","expires_in":300,"token_type":"Bearer"}`, n)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSingleFlightUnderConcurrency(t *testing.T) {
	var calls atomic.Int64
	srv := tokenServer(t, &calls, 50*time.Millisecond)
	s := NewSource(srv.URL, "provider-a", "s3cret", nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := s.Token(context.Background())
			if err != nil || tok != "tok-1" {
				t.Errorf("token = %q, %v", tok, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 || s.Fetches() != 1 {
		t.Fatalf("token endpoint called %d times, want 1", calls.Load())
	}
}

func TestBackgroundRefreshAtSeventyPercent(t *testing.T) {
	var calls atomic.Int64
	srv := tokenServer(t, &calls, 0)
	c := &clock{t: time.Unix(1_000_000, 0)}
	s := NewSource(srv.URL, "provider-a", "s3cret", nil)
	s.now = c.now
	ctx := context.Background()

	if tok, _ := s.Token(ctx); tok != "tok-1" {
		t.Fatalf("first token %q", tok)
	}
	c.add(200 * time.Second) // below 70% of 300 s
	if tok, _ := s.Token(ctx); tok != "tok-1" || calls.Load() != 1 {
		t.Fatalf("refreshed too early: %q after %d calls", tok, calls.Load())
	}
	c.add(15 * time.Second) // 215 s: past 210 s, still valid
	if tok, _ := s.Token(ctx); tok != "tok-1" {
		t.Fatalf("stale-but-valid token must be served while refreshing, got %q", tok)
	}
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	waitIdle(s)
	if tok, _ := s.Token(ctx); tok != "tok-2" {
		t.Fatalf("background refresh not applied, got %q", tok)
	}
}

func TestExpiredTokenIsFetchedSynchronouslyAndInvalidateWorks(t *testing.T) {
	var calls atomic.Int64
	srv := tokenServer(t, &calls, 0)
	c := &clock{t: time.Unix(1_000_000, 0)}
	s := NewSource(srv.URL, "provider-a", "s3cret", nil)
	s.now = c.now
	ctx := context.Background()

	tok, _ := s.Token(ctx)
	c.add(301 * time.Second)
	if got, _ := s.Token(ctx); got != "tok-2" {
		t.Fatalf("expired token served: %q", got)
	}
	s.Invalidate(tok) // an old token does not drop the current one
	if got, _ := s.Token(ctx); got != "tok-2" {
		t.Fatalf("invalidate of an old token dropped the new one: %q", got)
	}
	s.Invalidate("tok-2")
	if got, _ := s.Token(ctx); got != "tok-3" {
		t.Fatalf("after invalidate want tok-3, got %q", got)
	}
}

func TestErrorIsReported(t *testing.T) {
	var calls atomic.Int64
	srv := tokenServer(t, &calls, 0)
	s := NewSource(srv.URL, "provider-a", "wrong", nil)
	if _, err := s.Token(context.Background()); err == nil {
		t.Fatal("expected an error for invalid credentials")
	}
}

func waitIdle(s *Source) {
	for {
		s.mu.Lock()
		idle := s.inflight == nil
		s.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
