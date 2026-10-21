package verify

import (
	"strings"
	"testing"
)

func TestInvariantsFileParses(t *testing.T) {
	qs := Queries()
	if len(qs) < 10 {
		t.Fatalf("only %d invariants parsed", len(qs))
	}
	seen := map[string]bool{}
	for _, q := range qs {
		if seen[q.Name] {
			t.Errorf("duplicate invariant %s", q.Name)
		}
		seen[q.Name] = true
		if q.Description == "" || !strings.HasPrefix(strings.ToUpper(q.SQL), "SELECT") || strings.Contains(q.SQL, ";") {
			t.Errorf("invariant %s malformed: %q / %q", q.Name, q.Description, q.SQL)
		}
	}
}
