// Package target talks to the system under test: the wallet HTTP API on
// several instances and the inbound SQS FIFO queue. The types here mirror the
// public contract in ARCHITECTURE.md on purpose instead of importing the
// service, so the generator behaves like an external provider would.
package target

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// FormatMinor renders non-negative minor units in the canonical form the
// API accepts ("25.00").
func FormatMinor(minor int64) string {
	if minor < 0 {
		return "-" + FormatMinor(-minor)
	}
	return fmt.Sprintf("%d.%02d", minor/100, minor%100)
}

// ParseMinor parses "975.00" (optionally signed) into minor units, with
// integer arithmetic only.
func ParseMinor(s string) (int64, error) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, ok := strings.Cut(s, ".")
	if !ok || len(frac) != 2 || whole == "" {
		return 0, fmt.Errorf("amount %q is not in the canonical form", s)
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil || f < 0 {
		return 0, errors.New("invalid cents")
	}
	v := w*100 + f
	if neg {
		v = -v
	}
	return v, nil
}

func MoneyOf(minor int64, currency string) Money {
	return Money{Amount: FormatMinor(minor), Currency: currency}
}

// WagerRequest is the body of POST /wagering/transactions and the data of
// the SQS envelope (plus idempotencyKey there).
type WagerRequest struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey,omitempty"` // SQS only
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          Money  `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// Envelope is the SQS message body.
type Envelope struct {
	MessageID  string       `json:"messageId"`
	Type       string       `json:"type"`
	OccurredAt string       `json:"occurredAt"`
	Data       WagerRequest `json:"data"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WagerResponse covers 200, 202 and 422 bodies, and the error body of the
// other statuses.
type WagerResponse struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *Money    `json:"balance"`
	IdempotentReplay bool      `json:"idempotentReplay"`
	FailureCode      string    `json:"failureCode"`
	Error            *APIError `json:"error"`
}

type OpenWalletRequest struct {
	PlayerID       string `json:"playerId"`
	InitialBalance Money  `json:"initialBalance"`
}

type WalletResponse struct {
	ID       string    `json:"id"`
	PlayerID string    `json:"playerId"`
	Balance  Money     `json:"balance"`
	Version  int64     `json:"version"`
	Error    *APIError `json:"error"`
}

type ReconciliationResponse struct {
	WalletID          string `json:"walletId"`
	StoredBalance     Money  `json:"storedBalance"`
	CalculatedBalance Money  `json:"calculatedBalance"`
	Difference        Money  `json:"difference"`
	Consistent        bool   `json:"consistent"`
	CheckedEntries    int64  `json:"checkedEntries"`
}

// TransactionView is the subset of GET /providers/{p}/wagering/transactions/{id}
// the generator relies on.
type TransactionView struct {
	TransactionID string `json:"transactionId"`
	ID            string `json:"id"`
	Status        string `json:"status"`
	FailureCode   string `json:"failureCode"`
}

// TxID tolerates both "transactionId" and "id" in the lookup body.
func (v TransactionView) TxID() string {
	if v.TransactionID != "" {
		return v.TransactionID
	}
	return v.ID
}
