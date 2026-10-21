// Package failure defines the stable failure codes of the wallet contract and
// the error type that carries them. Codes are part of the public API: they are
// persisted, returned to providers and never change meaning.
package failure

import (
	"errors"
	"fmt"
)

type Code string

// Correctable codes: the input is wrong, nothing is persisted, and the client
// can fix the request and send it again.
const (
	InvalidRequest   Code = "INVALID_REQUEST"
	InvalidAmount    Code = "INVALID_AMOUNT"
	InvalidCurrency  Code = "INVALID_CURRENCY"
	UnsupportedKind  Code = "UNSUPPORTED_KIND"
	MissingReference Code = "MISSING_REFERENCE"
	WalletNotFound   Code = "WALLET_NOT_FOUND"
)

// Definitive codes: a business rule rejected the operation. The rejection is
// persisted and replays return it unchanged.
const (
	PlayerWalletMismatch      Code = "PLAYER_WALLET_MISMATCH"
	CurrencyMismatch          Code = "CURRENCY_MISMATCH"
	InsufficientFunds         Code = "INSUFFICIENT_FUNDS"
	ReversalInsufficientFunds Code = "REVERSAL_INSUFFICIENT_FUNDS"
	ReferenceNotFound         Code = "REFERENCE_NOT_FOUND"
	ReferenceNotProcessed     Code = "REFERENCE_NOT_PROCESSED"
	ReferenceMismatch         Code = "REFERENCE_MISMATCH"
	AmountMismatch            Code = "AMOUNT_MISMATCH"
	InvalidReferenceKind      Code = "INVALID_REFERENCE_KIND"
	AlreadyReversed           Code = "ALREADY_REVERSED"
	BalanceLimitExceeded      Code = "BALANCE_LIMIT_EXCEEDED"
)

// InvariantViolation is a permanent infrastructure failure: the database
// refused a state the domain accepted. It is recorded as FAILED for audit.
const InvariantViolation Code = "INVARIANT_VIOLATION"

var definitive = map[Code]bool{
	PlayerWalletMismatch: true, CurrencyMismatch: true, InsufficientFunds: true,
	ReversalInsufficientFunds: true, ReferenceNotFound: true, ReferenceNotProcessed: true,
	ReferenceMismatch: true, AmountMismatch: true, InvalidReferenceKind: true, AlreadyReversed: true,
	BalanceLimitExceeded: true,
}

// Definitive reports whether the code is a persisted business rejection.
func (c Code) Definitive() bool { return definitive[c] }

// Error is a classified domain error. Extract it with errors.As or test the
// code with Is.
type Error struct {
	Code    Code
	Message string
}

func New(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// Is matches another *Error with the same code, so errors.Is(err, failure.New(c, ""))
// and the helper Is below work through wrapping.
func (e *Error) Is(target error) bool {
	var t *Error
	return errors.As(target, &t) && t.Code == e.Code
}

// CodeOf returns the failure code carried by err, if any.
func CodeOf(err error) (Code, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Code, true
	}
	return "", false
}

// Is reports whether err carries the given code.
func Is(err error, code Code) bool {
	c, ok := CodeOf(err)
	return ok && c == code
}
