// Package auth validates OAuth 2.0 access tokens issued by the external IdP
// (Keycloak) and exposes the authenticated principal to the handlers.
//
// Tokens are validated locally: RS256 signature against the IdP's JWKS (cached,
// reloaded when an unknown kid shows up), issuer, audience, expiry and
// not-before with a small leeway. The IdP is not on the request path.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/fx"
)

// Roles of the realm (deploy/keycloak/wallet-realm.json).
const (
	RoleProvider = "wagering-provider"
	RoleInternal = "wallet-internal"
)

// Principal is the authenticated caller. ProviderID comes from a claim fixed
// per client in the IdP, so a provider cannot choose who it is.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      []string
}

func (p Principal) Has(role string) bool { return slices.Contains(p.Roles, role) }

var (
	ErrMissingToken = errors.New("auth: missing bearer token")
	ErrInvalidToken = errors.New("auth: invalid token")
)

type claims struct {
	jwt.RegisteredClaims
	ProviderID string   `json:"provider_id"`
	Roles      []string `json:"roles"`
	ClientID   string   `json:"azp"`
}

// Verifier validates tokens. It is safe for concurrent use.
type Verifier struct {
	issuer   string
	audience string
	keys     *JWKS
	parser   *jwt.Parser
}

func NewVerifier(issuer, audience string, keys *JWKS, leeway time.Duration) *Verifier {
	return &Verifier{
		issuer: issuer, audience: audience, keys: keys,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithIssuer(issuer),
			jwt.WithAudience(audience),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(leeway),
		),
	}
}

func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	var c claims
	_, err := v.parser.ParseWithClaims(raw, &c, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.keys.Key(ctx, kid)
	})
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	return Principal{Subject: c.Subject, ClientID: c.ClientID, ProviderID: c.ProviderID, Roles: c.Roles}, nil
}

// JWKS caches the IdP signing keys.
type JWKS struct {
	url    string
	client *http.Client
	log    *slog.Logger

	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

// minRefresh bounds how often an unknown kid can trigger a reload, so a flood
// of forged tokens cannot turn into a flood of requests to the IdP.
const minRefresh = 10 * time.Second

func NewJWKS(url string, log *slog.Logger) *JWKS {
	return &JWKS{url: url, client: &http.Client{Timeout: 5 * time.Second}, log: log, keys: map[string]*rsa.PublicKey{}}
}

func (j *JWKS) Key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if k, ok := j.keys[kid]; ok {
		return k, nil
	}
	if time.Since(j.fetchedAt) < minRefresh {
		return nil, fmt.Errorf("unknown key id %q", kid)
	}
	if err := j.refreshLocked(ctx); err != nil {
		return nil, err
	}
	if k, ok := j.keys[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("unknown key id %q", kid)
}

// Refresh reloads the keys now.
func (j *JWKS) Refresh(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.refreshLocked(ctx)
}

func (j *JWKS) refreshLocked(ctx context.Context) error {
	j.fetchedAt = time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	if err != nil {
		return err
	}
	resp, err := j.client.Do(req)
	if err != nil {
		return fmt.Errorf("jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks: status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return fmt.Errorf("jwks: decode: %w", err)
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		n, err1 := base64.RawURLEncoding.DecodeString(k.N)
		e, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil {
			continue
		}
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	}
	if len(keys) == 0 {
		return errors.New("jwks: no RSA signing key")
	}
	j.keys = keys
	j.log.Info("jwks loaded", "keys", len(keys))
	return nil
}

func NewJWKSFromConfig(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) *JWKS {
	j := NewJWKS(cfg.OIDCJWKSURL, log)
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		// Warm the cache, but do not fail the start: SQS processing and the
		// health checks do not depend on the IdP, and the keys load lazily on
		// the first request otherwise.
		if err := j.Refresh(ctx); err != nil {
			log.Warn("jwks not loaded at start; will retry on first request", "error", err)
		}
		return nil
	}})
	return j
}

func NewVerifierFromConfig(cfg config.Config, keys *JWKS) *Verifier {
	return NewVerifier(cfg.OIDCIssuerURL, cfg.OIDCAudience, keys, cfg.OIDCClockSkew)
}

var Module = fx.Module("auth", fx.Provide(NewJWKSFromConfig, NewVerifierFromConfig))
