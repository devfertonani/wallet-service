// Package money implements an immutable monetary value object.
//
// Amounts are stored as int64 minor units (cents) with a fixed scale of two
// decimal places. No floating point type is used anywhere: parsing, arithmetic,
// comparison and formatting work on integers and decimal strings only.
//
// Limits: the representable range is [math.MinInt64, math.MaxInt64] minor
// units, i.e. roughly ±92 233 720 368 547 758.07. Every operation that could
// leave that range returns ErrOverflow instead of wrapping.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Scale is the fixed number of decimal places of every supported currency.
const Scale = 2

const minorPerUnit = 100

var (
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrNegativeAmount   = errors.New("money: negative amount")
	ErrOverflow         = errors.New("money: overflow")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrUninitialized    = errors.New("money: uninitialized value")
)

// Currency is an ISO 4217 alphabetic code whose minor unit is two decimals.
type Currency struct{ code string }

// supported lists the ISO 4217 currencies accepted by the service. All of them
// use two decimal places, which keeps Scale fixed; currencies with another
// minor unit (JPY, CLP, KWD) are out of scope on purpose.
var supported = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "MXN": {}, "ARS": {},
}

// ParseCurrency validates an ISO 4217 code. It is case sensitive: "brl" is
// rejected instead of being normalized silently.
func ParseCurrency(code string) (Currency, error) {
	if _, ok := supported[code]; !ok {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return Currency{code: code}, nil
}

// MustCurrency is meant for constants in tests and wiring, never for input.
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

func (c Currency) String() string { return c.code }
func (c Currency) IsZero() bool   { return c.code == "" }

// Money is an immutable amount in a currency. The zero value is invalid and is
// rejected by every operation through ErrUninitialized.
type Money struct {
	minor    int64
	currency Currency
}

// Zero returns zero in the given currency.
func Zero(c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrUninitialized
	}
	return Money{currency: c}, nil
}

// FromMinor builds Money from minor units. Used for rehydration from storage,
// where the value has already been validated once.
func FromMinor(minor int64, c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrUninitialized
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse reads a decimal string with exactly two decimal places and an optional
// leading minus sign ("-12.50"). It is the parser for internal values.
// External financial input must go through ParseNonNegative.
func Parse(amount, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

// ParseNonNegative is Parse plus the rejection of negative values, for amounts
// received from providers and API clients.
func ParseNonNegative(amount, currency string) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	return Parse(amount, currency)
}

// parseMinor accepts only the canonical form: optional "-", an integer part
// without leading zeros, a dot and exactly Scale digits. Everything else
// ("1", "1.5", "1.005", "01.00", "+1.00", "1e2", "NaN", "Infinity", " 1.00",
// "-0.00") is rejected instead of being rounded or normalized, so the string a
// client sends is the string that goes into the idempotency hash.
func parseMinor(s string) (int64, error) {
	invalid := func() (int64, error) { return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s) }

	neg := false
	body := s
	if strings.HasPrefix(body, "-") {
		neg = true
		body = body[1:]
	}
	dot := strings.IndexByte(body, '.')
	if dot <= 0 || len(body)-dot-1 != Scale {
		return invalid()
	}
	intPart, fracPart := body[:dot], body[dot+1:]
	if len(intPart) > 1 && intPart[0] == '0' {
		return invalid()
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return invalid()
	}

	// Accumulate as a negative number: the negative range of int64 is one
	// larger than the positive one, so math.MinInt64 stays parseable.
	var acc int64
	for _, r := range intPart + fracPart {
		d := int64(r - '0')
		if acc < (math.MinInt64+d)/10 {
			return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
		}
		acc = acc*10 - d
	}
	if neg {
		if acc == 0 {
			return invalid() // "-0.00" is not canonical
		}
		return acc, nil
	}
	if acc == math.MinInt64 {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
	}
	return -acc, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (m Money) valid() error {
	if m.currency.IsZero() {
		return ErrUninitialized
	}
	return nil
}

func (m Money) sameCurrency(o Money) error {
	if err := m.valid(); err != nil {
		return err
	}
	if err := o.valid(); err != nil {
		return err
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s and %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.sameCurrency(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Neg returns -m.
func (m Money) Neg() (Money, error) {
	if err := m.valid(); err != nil {
		return Money{}, err
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp returns -1, 0 or +1 comparing m with o.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.sameCurrency(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether both values have the same amount and currency.
func (m Money) Equal(o Money) bool { return m.minor == o.minor && m.currency == o.currency }

func (m Money) IsZero() bool       { return m.minor == 0 }
func (m Money) IsNegative() bool   { return m.minor < 0 }
func (m Money) IsPositive() bool   { return m.minor > 0 }
func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsValid() bool      { return m.valid() == nil }

// Amount formats the value as a decimal string with exactly two places.
func (m Money) Amount() string {
	u := uint64(m.minor)
	sign := ""
	if m.minor < 0 {
		u = uint64(-(m.minor + 1)) + 1 // safe for math.MinInt64
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%02d", sign, u/minorPerUnit, u%minorPerUnit)
}

func (m Money) String() string { return m.Amount() + " " + m.currency.String() }

// DTO is the external JSON contract: {"amount":"25.00","currency":"BRL"}.
type DTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) DTO() DTO { return DTO{Amount: m.Amount(), Currency: m.currency.String()} }

// MarshalJSON serializes the value; an uninitialized value is an error rather
// than a silent "0.00".
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.valid(); err != nil {
		return nil, err
	}
	return json.Marshal(m.DTO())
}

// UnmarshalJSON accepts the external contract, including negative values; use
// ParseNonNegative on a DTO where the input must not be negative.
func (m *Money) UnmarshalJSON(b []byte) error {
	var d DTO
	if err := json.Unmarshal(b, &d); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	v, err := Parse(d.Amount, d.Currency)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
