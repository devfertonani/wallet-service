package wager

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode"

	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/google/uuid"
)

// MaxIdentifierLength bounds every free-form identifier sent by a provider.
const MaxIdentifierLength = 128

// CommandInput is the raw external operation, as received by HTTP or SQS.
// Both channels build it the same way, so they share validation and hash.
type CommandInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// Command is a validated external operation. Its fields are read-only.
type Command struct {
	providerID       string
	externalID       string
	idempotencyKey   string
	playerID         uuid.UUID
	walletID         uuid.UUID
	roundID          string
	gameID           string
	kind             Kind
	money            money.Money
	referenceExterID string
	payloadHash      string
}

// NewCommand validates the input. Every error is a correctable *failure.Error:
// nothing is persisted for an invalid command.
func NewCommand(in CommandInput) (Command, error) {
	for _, f := range []struct{ name, value string }{
		{"providerId", in.ProviderID}, {"externalTransactionId", in.ExternalTransactionID},
		{"idempotencyKey", in.IdempotencyKey}, {"roundId", in.RoundID}, {"gameId", in.GameID},
	} {
		if err := identifier(f.name, f.value); err != nil {
			return Command{}, err
		}
	}
	player, err := uuid.Parse(in.PlayerID)
	if err != nil || player == uuid.Nil {
		return Command{}, failure.New(failure.InvalidRequest, "playerId must be a UUID")
	}
	wallet, err := uuid.Parse(in.WalletID)
	if err != nil || wallet == uuid.Nil {
		return Command{}, failure.New(failure.InvalidRequest, "walletId must be a UUID")
	}
	kind, err := ParseKind(in.Kind)
	if err != nil {
		return Command{}, err
	}
	if !kind.External() {
		return Command{}, failure.New(failure.UnsupportedKind, "kind %s is internal only", kind)
	}
	m, err := money.ParseNonNegative(in.Amount, in.Currency)
	switch {
	case errors.Is(err, money.ErrInvalidCurrency):
		return Command{}, failure.New(failure.InvalidCurrency, "unsupported currency %q", in.Currency)
	case err != nil:
		return Command{}, failure.New(failure.InvalidAmount, "%v", err)
	}
	// Zero policy: LOSS is exactly zero, every other kind moves a positive amount.
	if kind == Loss && !m.IsZero() {
		return Command{}, failure.New(failure.InvalidAmount, "LOSS amount must be 0.00")
	}
	if kind != Loss && !m.IsPositive() {
		return Command{}, failure.New(failure.InvalidAmount, "%s amount must be greater than zero", kind)
	}
	if in.ReferenceExternalTransactionID != "" {
		if err := identifier("referenceExternalTransactionId", in.ReferenceExternalTransactionID); err != nil {
			return Command{}, err
		}
		if kind == Bet || kind == Loss {
			return Command{}, failure.New(failure.InvalidRequest, "%s does not take a reference", kind)
		}
		if in.ReferenceExternalTransactionID == in.ExternalTransactionID {
			return Command{}, failure.New(failure.InvalidRequest, "an operation cannot reference itself")
		}
	} else if kind.RequiresReference() {
		return Command{}, failure.New(failure.MissingReference, "%s requires referenceExternalTransactionId", kind)
	}
	c := Command{
		providerID: in.ProviderID, externalID: in.ExternalTransactionID, idempotencyKey: in.IdempotencyKey,
		playerID: player, walletID: wallet, roundID: in.RoundID, gameID: in.GameID, kind: kind, money: m,
		referenceExterID: in.ReferenceExternalTransactionID,
	}
	c.payloadHash = c.hash()
	return c, nil
}

func identifier(name, v string) error {
	if v == "" {
		return failure.New(failure.InvalidRequest, "%s is required", name)
	}
	if len(v) > MaxIdentifierLength {
		return failure.New(failure.InvalidRequest, "%s is longer than %d bytes", name, MaxIdentifierLength)
	}
	for _, r := range v {
		if r == unicode.ReplacementChar || !unicode.IsPrint(r) || unicode.IsSpace(r) {
			return failure.New(failure.InvalidRequest, "%s contains invalid characters", name)
		}
	}
	return nil
}

// HashVersion prefixes every stored payload hash, so the algorithm can change
// without invalidating hashes already persisted.
const HashVersion = "sha256:v1:"

// hash is SHA-256 over the canonical JSON of the business fields: keys in
// lexicographic order (encoding/json sorts map keys), no insignificant
// whitespace, no HTML escaping, absent optional fields omitted. The
// idempotency key and every transport field (messageId, occurredAt, headers,
// token, correlation id) stay out. UUIDs are in canonical lowercase form; money
// needs no normalization because only the canonical form is accepted.
func (c Command) hash() string {
	fields := map[string]any{
		"providerId":            c.providerID,
		"externalTransactionId": c.externalID,
		"playerId":              c.playerID.String(),
		"walletId":              c.walletID.String(),
		"roundId":               c.roundID,
		"gameId":                c.gameID,
		"kind":                  string(c.kind),
		"money":                 map[string]string{"amount": c.money.Amount(), "currency": c.money.Currency().String()},
	}
	if c.referenceExterID != "" {
		fields["referenceExternalTransactionId"] = c.referenceExterID
	}
	sum := sha256.Sum256(CanonicalJSON(fields))
	return HashVersion + hex.EncodeToString(sum[:])
}

// CanonicalJSON encodes v with sorted keys, without HTML escaping and without
// the trailing newline of json.Encoder.
func CanonicalJSON(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Only maps of strings reach here; failing to encode them is a bug.
		panic(err)
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}

func (c Command) ProviderID() string                     { return c.providerID }
func (c Command) ExternalTransactionID() string          { return c.externalID }
func (c Command) IdempotencyKey() string                 { return c.idempotencyKey }
func (c Command) PlayerID() uuid.UUID                    { return c.playerID }
func (c Command) WalletID() uuid.UUID                    { return c.walletID }
func (c Command) RoundID() string                        { return c.roundID }
func (c Command) GameID() string                         { return c.gameID }
func (c Command) Kind() Kind                             { return c.kind }
func (c Command) Money() money.Money                     { return c.money }
func (c Command) ReferenceExternalTransactionID() string { return c.referenceExterID }
func (c Command) PayloadHash() string                    { return c.payloadHash }

// String is a short description for logs; it carries no credential.
func (c Command) String() string {
	return strings.Join([]string{c.providerID, c.externalID, string(c.kind), c.money.String()}, " ")
}
