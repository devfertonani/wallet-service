package config

import (
	"strings"
	"testing"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://wallet_service:wallet_service@localhost:55432/wallet")
	t.Setenv("OIDC_ISSUER_URL", "http://localhost:8081/realms/wallet")
	t.Setenv("OIDC_JWKS_URL", "http://keycloak:8080/realms/wallet/protocol/openid-connect/certs")
}

// Every variable that validate requires must also be read, or the service
// never starts.
func TestLoadReadsEveryRequiredVariable(t *testing.T) {
	setRequired(t)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.OIDCJWKSURL != "http://keycloak:8080/realms/wallet/protocol/openid-connect/certs" {
		t.Errorf("OIDCJWKSURL = %q", c.OIDCJWKSURL)
	}
	if c.OIDCIssuerURL != "http://localhost:8081/realms/wallet" {
		t.Errorf("OIDCIssuerURL = %q", c.OIDCIssuerURL)
	}
}

func TestLoadRejectsMissingRequiredVariables(t *testing.T) {
	for _, key := range []string{"DATABASE_URL", "OIDC_ISSUER_URL", "OIDC_JWKS_URL"} {
		t.Run(key, func(t *testing.T) {
			setRequired(t)
			t.Setenv(key, "")
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("Load without %s: err = %v", key, err)
			}
		})
	}
}
