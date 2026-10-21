// Package auth fetches OAuth2 client_credentials tokens and keeps them off
// the critical path: a token is cached, refreshed in the background at 70%
// of its lifetime and fetched by a single caller at a time (single flight).
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RefreshFraction of the token lifetime after which a refresh starts.
const RefreshFraction = 0.7

type Source struct {
	tokenURL, clientID, secret string
	httpc                      *http.Client
	now                        func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
	refreshAt time.Time
	inflight  chan struct{}
	lastErr   error

	fetches atomic.Int64
}

func NewSource(tokenURL, clientID, secret string, httpc *http.Client) *Source {
	if httpc == nil {
		httpc = &http.Client{Timeout: 10 * time.Second}
	}
	return &Source{tokenURL: tokenURL, clientID: clientID, secret: secret, httpc: httpc, now: time.Now}
}

func (s *Source) ClientID() string { return s.clientID }

// Fetches is how many times the token endpoint was called.
func (s *Source) Fetches() int64 { return s.fetches.Load() }

// Token returns a valid access token, fetching one only when needed.
func (s *Source) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	now := s.now()
	if s.token != "" && now.Before(s.expiresAt) {
		tok := s.token
		if !now.Before(s.refreshAt) && s.inflight == nil {
			s.startLocked() // still valid: refresh in the background
		}
		s.mu.Unlock()
		return tok, nil
	}
	ch := s.inflight
	if ch == nil {
		ch = s.startLocked()
	}
	s.mu.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && s.now().Before(s.expiresAt) {
		return s.token, nil
	}
	if s.lastErr != nil {
		return "", s.lastErr
	}
	return "", errors.New("auth: token endpoint returned an already expired token")
}

// Invalidate drops tok if it is still the cached token, after the server
// answered 401 for it, so the next call fetches a fresh one.
func (s *Source) Invalidate(tok string) {
	s.mu.Lock()
	if s.token == tok {
		s.token = ""
	}
	s.mu.Unlock()
}

func (s *Source) startLocked() chan struct{} {
	ch := make(chan struct{})
	s.inflight = ch
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		tok, ttl, err := s.fetch(ctx)
		s.mu.Lock()
		if err == nil {
			now := s.now()
			s.token = tok
			s.expiresAt = now.Add(ttl)
			s.refreshAt = now.Add(time.Duration(float64(ttl) * RefreshFraction))
		}
		s.lastErr = err
		s.inflight = nil
		close(ch)
		s.mu.Unlock()
	}()
	return ch
}

func (s *Source) fetch(ctx context.Context) (string, time.Duration, error) {
	s.fetches.Add(1)
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {s.clientID}, "client_secret": {s.secret}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.httpc.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("auth: token request for %s: %w", s.clientID, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("auth: token request for %s: HTTP %d: %.200s", s.clientID, resp.StatusCode, body)
	}
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.AccessToken == "" {
		return "", 0, fmt.Errorf("auth: token response for %s is not a token: %.200s", s.clientID, body)
	}
	if out.ExpiresIn <= 0 {
		out.ExpiresIn = 60
	}
	return out.AccessToken, time.Duration(out.ExpiresIn) * time.Second, nil
}
