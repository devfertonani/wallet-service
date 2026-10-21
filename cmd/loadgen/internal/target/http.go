package target

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/devfertonani/wallet-service/cmd/loadgen/internal/auth"
)

// RetryPolicy is what a well behaved provider does: transient failures (503,
// 502, 504, 429, timeouts, refused connections) are retried with the same
// Idempotency-Key and body, with jittered exponential backoff.
type RetryPolicy struct {
	MaxAttempts    int
	Base           time.Duration
	Cap            time.Duration
	AttemptTimeout time.Duration
}

func DefaultRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 6, Base: 50 * time.Millisecond, Cap: 2 * time.Second, AttemptTimeout: 5 * time.Second}
}

// Backoff for retry n (1-based): full jitter over base*2^(n-1), capped, but
// never below the server's Retry-After when it sent one.
func (p RetryPolicy) Backoff(n int, retryAfter time.Duration) time.Duration {
	ceil := p.Base << uint(min(n-1, 20))
	if ceil > p.Cap || ceil <= 0 {
		ceil = p.Cap
	}
	d := time.Duration(rand.Int64N(int64(ceil) + 1))
	if retryAfter > 0 {
		ra := min(retryAfter, p.Cap)
		d = ra + time.Duration(rand.Int64N(int64(p.Base)+1))
	}
	return d
}

// HTTP is a keep-alive client that spreads requests round robin over the
// wallet instances (there is no load balancer in the Compose environment).
type HTTP struct {
	Targets []string
	Client  *http.Client
	Retry   RetryPolicy
	rr      atomic.Uint64
	retries atomic.Int64
}

func NewHTTP(targets []string, retry RetryPolicy) *HTTP {
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   false,
	}
	return &HTTP{Targets: targets, Client: &http.Client{Transport: tr}, Retry: retry}
}

// Retries is the number of extra attempts made so far.
func (h *HTTP) Retries() int64 { return h.retries.Load() }

// Request describes one logical call. Pin chooses the instance of the first
// attempt (-1 for round robin); retries always move to the next instance, so
// a killed instance does not hold the operation back.
type Request struct {
	Method  string
	Path    string
	Token   *auth.Source
	Headers map[string]string
	Body    []byte
	Pin     int
	// NoRetry disables the transient retry loop (used by probes).
	NoRetry bool
}

type Response struct {
	Status   int
	Body     []byte
	Attempts int
	Target   string // instance that produced the final answer
	Err      error  // final transport error, when no HTTP answer was obtained
	// Retried503 counts 503 answers seen before the final one.
	Retried503 int
}

func retryable(status int) bool {
	return status == http.StatusServiceUnavailable || status == http.StatusBadGateway ||
		status == http.StatusGatewayTimeout || status == http.StatusTooManyRequests
}

func (h *HTTP) pick(pin, attempt int) int {
	if pin >= 0 {
		return (pin + attempt) % len(h.Targets)
	}
	return int(h.rr.Add(1)-1) % len(h.Targets)
}

// Do executes the request with the retry policy and returns the final
// outcome. It never returns early on a transient failure unless ctx ends.
func (h *HTTP) Do(ctx context.Context, r Request) Response {
	var res Response
	max := h.Retry.MaxAttempts
	if r.NoRetry || max < 1 {
		max = 1
	}
	refreshed := false
	for attempt := 0; attempt < max; attempt++ {
		if attempt > 0 {
			h.retries.Add(1)
		}
		res.Attempts = attempt + 1
		base := h.Targets[h.pick(r.Pin, attempt)]
		res.Target = base
		status, body, hdr, tok, err := h.once(ctx, base, r)
		if err == nil && status == http.StatusUnauthorized && r.Token != nil && !refreshed {
			// The cached token may have expired between check and use.
			r.Token.Invalidate(tok)
			refreshed = true
			attempt--
			continue
		}
		if err == nil && !retryable(status) {
			res.Status, res.Body, res.Err = status, body, nil
			return res
		}
		var retryAfter time.Duration
		if err == nil {
			res.Status, res.Body, res.Err = status, body, nil
			if s, perr := strconv.Atoi(hdr.Get("Retry-After")); perr == nil && s > 0 {
				retryAfter = time.Duration(s) * time.Second
			}
		} else {
			res.Status, res.Body, res.Err = 0, nil, err
		}
		if ctx.Err() != nil || attempt == max-1 {
			break
		}
		if res.Status == http.StatusServiceUnavailable {
			res.Retried503++
		}
		t := time.NewTimer(h.Retry.Backoff(attempt+1, retryAfter))
		select {
		case <-ctx.Done():
			t.Stop()
			if res.Err == nil && res.Status == 0 {
				res.Err = ctx.Err()
			}
			return res
		case <-t.C:
		}
	}
	return res
}

func (h *HTTP) once(ctx context.Context, base string, r Request) (int, []byte, http.Header, string, error) {
	actx, cancel := context.WithTimeout(ctx, h.Retry.AttemptTimeout)
	defer cancel()
	var body io.Reader
	if r.Body != nil {
		body = bytes.NewReader(r.Body)
	}
	req, err := http.NewRequestWithContext(actx, r.Method, base+r.Path, body)
	if err != nil {
		return 0, nil, nil, "", err
	}
	if r.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}
	var tok string
	if r.Token != nil {
		tok, err = r.Token.Token(actx)
		if err != nil {
			return 0, nil, nil, "", err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return 0, nil, nil, tok, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, nil, tok, err
	}
	return resp.StatusCode, b, resp.Header, tok, nil
}

// IsTimeout reports whether err is a deadline or network timeout, which
// leaves the outcome of a write unknown.
func IsTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}
