package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

var brl = MustCurrency("BRL")

func TestParseAcceptsCanonicalForm(t *testing.T) {
	cases := map[string]int64{
		"0.00":                  0,
		"0.01":                  1,
		"25.00":                 2500,
		"1000.99":               100099,
		"-12.50":                -1250,
		"92233720368547758.07":  math.MaxInt64,
		"-92233720368547758.08": math.MinInt64,
	}
	for in, want := range cases {
		m, err := Parse(in, "BRL")
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", in, err)
		}
		if m.Minor() != want {
			t.Errorf("Parse(%q) = %d, want %d", in, m.Minor(), want)
		}
		if m.Amount() != in {
			t.Errorf("round trip of %q gave %q", in, m.Amount())
		}
	}
}

func TestParseRejectsNonCanonicalInput(t *testing.T) {
	for _, in := range []string{
		"", " ", "1", "1.", ".50", "1.5", "1.005", "01.00", "00.00", "+1.00",
		"1e2", "1E2", "1.0e1", "NaN", "nan", "Infinity", "-Infinity", "inf",
		" 1.00", "1.00 ", "1,00", "1_000.00", "--1.00", "-0.00", "0x10.00", "١.٠٠",
	} {
		if _, err := Parse(in, "BRL"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("Parse(%q) error = %v, want ErrInvalidAmount", in, err)
		}
	}
}

func TestParseOverflow(t *testing.T) {
	for _, in := range []string{"92233720368547758.08", "-92233720368547758.09", "99999999999999999999.00"} {
		if _, err := Parse(in, "BRL"); !errors.Is(err, ErrOverflow) {
			t.Errorf("Parse(%q) error = %v, want ErrOverflow", in, err)
		}
	}
}

func TestParseNonNegativeRejectsNegative(t *testing.T) {
	if _, err := ParseNonNegative("-1.00", "BRL"); !errors.Is(err, ErrNegativeAmount) {
		t.Fatalf("got %v, want ErrNegativeAmount", err)
	}
	if m, err := ParseNonNegative("0.00", "BRL"); err != nil || !m.IsZero() {
		t.Fatalf("zero must be accepted, got %v %v", m, err)
	}
}

func TestParseCurrency(t *testing.T) {
	for _, in := range []string{"", "brl", "BR", "BRLL", "XXX", "JPY"} {
		if _, err := ParseCurrency(in); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) error = %v, want ErrInvalidCurrency", in, err)
		}
	}
}

func mustParse(t *testing.T, a, c string) Money {
	t.Helper()
	m, err := Parse(a, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestArithmetic(t *testing.T) {
	a := mustParse(t, "100.00", "BRL")
	b := mustParse(t, "80.00", "BRL")

	sum, _ := a.Add(b)
	diff, _ := b.Sub(a)
	neg, _ := a.Neg()
	if sum.Amount() != "180.00" || diff.Amount() != "-20.00" || neg.Amount() != "-100.00" {
		t.Fatalf("got sum=%s diff=%s neg=%s", sum.Amount(), diff.Amount(), neg.Amount())
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Errorf("Cmp(100, 80) = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Errorf("Cmp(80, 100) = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Errorf("Cmp(100, 100) = %d", c)
	}
	if !a.Equal(mustParse(t, "100.00", "BRL")) {
		t.Error("Equal must compare amount and currency")
	}
}

func TestArithmeticOverflow(t *testing.T) {
	max, _ := FromMinor(math.MaxInt64, brl)
	min, _ := FromMinor(math.MinInt64, brl)
	one, _ := FromMinor(1, brl)

	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("max+1: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("min-1: %v", err)
	}
	minusOne, _ := one.Neg()
	if _, err := max.Sub(minusOne); !errors.Is(err, ErrOverflow) {
		t.Errorf("max-(-1): %v", err)
	}
	if _, err := min.Add(minusOne); !errors.Is(err, ErrOverflow) {
		t.Errorf("min+(-1): %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, ErrOverflow) {
		t.Errorf("-min: %v", err)
	}
	if got := min.Amount(); got != "-92233720368547758.08" {
		t.Errorf("min.Amount() = %s", got)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	a := mustParse(t, "1.00", "BRL")
	b := mustParse(t, "1.00", "USD")
	if _, err := a.Add(b); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := a.Sub(b); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := a.Cmp(b); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
	if a.Equal(b) {
		t.Error("Equal across currencies must be false")
	}
}

func TestUninitializedValueIsRejected(t *testing.T) {
	var zero Money
	one := mustParse(t, "1.00", "BRL")
	if _, err := zero.Add(one); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Add: %v", err)
	}
	if _, err := one.Sub(zero); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Neg: %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Error("Marshal of uninitialized Money must fail")
	}
	if _, err := Zero(Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Zero: %v", err)
	}
}

func TestJSONContract(t *testing.T) {
	m := mustParse(t, "25.00", "BRL")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("Marshal = %s", b)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("Unmarshal = %v, %v", back, err)
	}
}

func TestJSONRejectsNumbersAndBadStrings(t *testing.T) {
	for _, in := range []string{
		`{"amount":25.00,"currency":"BRL"}`,
		`{"amount":"25","currency":"BRL"}`,
		`{"amount":"2.5e1","currency":"BRL"}`,
		`{"amount":"NaN","currency":"BRL"}`,
		`{"amount":"25.00","currency":"brl"}`,
		`{"amount":"25.00"}`,
		`{}`,
	} {
		var m Money
		if err := json.Unmarshal([]byte(in), &m); err == nil {
			t.Errorf("Unmarshal(%s) must fail, got %v", in, m)
		}
	}
}
