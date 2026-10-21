package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/platform/auth"
	"github.com/golang-jwt/jwt/v5"
)

const (
	issuer   = "http://idp.test/realms/wallet"
	audience = "wallet-api"
)

type idp struct {
	key     *rsa.PrivateKey
	kid     string
	fetches atomic.Int32
	srv     *httptest.Server
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	i := &idp{key: key, kid: "k1"}
	i.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i.fetches.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kid": i.kid, "kty": "RSA", "use": "sig", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	t.Cleanup(i.srv.Close)
	return i
}

func (i *idp) verifier() *auth.Verifier {
	return auth.NewVerifier(issuer, audience, auth.NewJWKS(i.srv.URL, slog.New(slog.NewTextHandler(io.Discard, nil))), 0)
}

func (i *idp) sign(t *testing.T, mutate func(jwt.MapClaims, *jwt.Token)) string {
	t.Helper()
	c := jwt.MapClaims{
		"iss": issuer, "aud": audience, "sub": "svc", "azp": "provider-a",
		"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(),
		"provider_id": "provider-a", "roles": []string{auth.RoleProvider},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	tok.Header["kid"] = i.kid
	if mutate != nil {
		mutate(c, tok)
	}
	s, err := tok.SignedString(i.key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestValidToken(t *testing.T) {
	i := newIDP(t)
	p, err := i.verifier().Verify(context.Background(), i.sign(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if p.ProviderID != "provider-a" || p.ClientID != "provider-a" || !p.Has(auth.RoleProvider) || p.Has(auth.RoleInternal) {
		t.Fatalf("principal = %+v", p)
	}
}

func TestRejectedTokens(t *testing.T) {
	i := newIDP(t)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	cases := map[string]string{
		"expired":         i.sign(t, func(c jwt.MapClaims, _ *jwt.Token) { c["exp"] = time.Now().Add(-time.Minute).Unix() }),
		"no expiry":       i.sign(t, func(c jwt.MapClaims, _ *jwt.Token) { delete(c, "exp") }),
		"not yet valid":   i.sign(t, func(c jwt.MapClaims, _ *jwt.Token) { c["nbf"] = time.Now().Add(time.Hour).Unix() }),
		"wrong issuer":    i.sign(t, func(c jwt.MapClaims, _ *jwt.Token) { c["iss"] = "http://evil.test/realms/wallet" }),
		"wrong audience":  i.sign(t, func(c jwt.MapClaims, _ *jwt.Token) { c["aud"] = "another-api" }),
		"unknown key id":  i.sign(t, func(_ jwt.MapClaims, tok *jwt.Token) { tok.Header["kid"] = "nope" }),
		"garbage":         "a.b.c",
		"empty signature": func() string { s := i.sign(t, nil); return s[:len(s)-10] }(),
	}
	// Signed with a key the IdP does not publish, but claiming its kid.
	forged := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": issuer, "aud": audience, "exp": time.Now().Add(time.Minute).Unix()})
	forged.Header["kid"] = i.kid
	cases["foreign key"], _ = forged.SignedString(other)
	// alg=none and HS256 signed with the public key (algorithm confusion).
	none := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"iss": issuer, "aud": audience, "exp": time.Now().Add(time.Minute).Unix()})
	cases["alg none"], _ = none.SignedString(jwt.UnsafeAllowNoneSignatureType)
	hs := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": issuer, "aud": audience, "exp": time.Now().Add(time.Minute).Unix()})
	hs.Header["kid"] = i.kid
	cases["HS256 confusion"], _ = hs.SignedString(i.key.PublicKey.N.Bytes())

	v := i.verifier()
	for name, tok := range cases {
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, auth.ErrInvalidToken) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestUnknownKidRefreshIsRateLimited(t *testing.T) {
	i := newIDP(t)
	v := i.verifier()
	if _, err := v.Verify(context.Background(), i.sign(t, nil)); err != nil {
		t.Fatal(err)
	}
	bad := i.sign(t, func(_ jwt.MapClaims, tok *jwt.Token) { tok.Header["kid"] = "rotated" })
	for range 20 {
		_, _ = v.Verify(context.Background(), bad)
	}
	if n := i.fetches.Load(); n != 1 {
		t.Fatalf("JWKS fetched %d times; a flood of unknown kids must not reach the IdP", n)
	}
}
