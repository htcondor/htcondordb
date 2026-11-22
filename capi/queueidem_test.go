package main

import (
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// matches builds an ad carrying have as its IdempotencyKey, and reports what the lookup's
// constraint for want evaluates to against it. The constraint is exercised through a real
// ClassAd evaluation rather than compared as text, because what matters is the expression
// the server will evaluate, not how it was spelled.
func matches(t *testing.T, want, have string) bool {
	t.Helper()
	cons, err := idemConstraint("IdempotencyKey", want)
	if err != nil {
		t.Fatalf("idemConstraint(%q): %v", want, err)
	}
	// ParseExpr, not EvaluateExprString: that helper first tries to read its input as an
	// attribute assignment, so it rejects every comparison expression, "==" included. The
	// constraint itself travels to the server as text and is parsed there as an expression,
	// which is what ParseExpr does here.
	expr, perr := classad.ParseExpr(cons)
	if perr != nil {
		t.Fatalf("parsing %q: %v", cons, perr)
	}
	ad := classad.New()
	ad.InsertAttrString("IdempotencyKey", have)
	ad.InsertAttr("ClusterId", 7)
	v := ad.EvaluateExprWithTarget(expr, nil)
	b, berr := v.BoolValue()
	if berr != nil {
		t.Fatalf("constraint %q evaluated to a non-boolean: %v", cons, berr)
	}
	return b
}

// A key is matched exactly, and only itself.
func TestIdemConstraintMatchesItsOwnKey(t *testing.T) {
	if !matches(t, "nightly-2026-11-21", "nightly-2026-11-21") {
		t.Error("a key does not match itself")
	}
	if matches(t, "nightly-2026-11-21", "nightly-2026-11-22") {
		t.Error("a key matched a different key")
	}
}

// Case matters. ClassAd's == folds case on strings, so a constraint written with == would
// make "Batch-7" and "batch-7" the same key and refuse the second submission. =?= does not,
// and this is the test that tells the two operators apart: it fails if the constraint is
// rewritten to use ==.
func TestIdemConstraintIsCaseSensitive(t *testing.T) {
	if matches(t, "Batch-7", "batch-7") {
		t.Error("keys differing only in case were treated as the same key; the constraint must use =?=, not ==")
	}
	if !strings.Contains(mustConstraint(t, "Batch-7"), "=?=") {
		t.Error("the constraint does not use =?=")
	}
}

func mustConstraint(t *testing.T, key string) string {
	t.Helper()
	c, err := idemConstraint("IdempotencyKey", key)
	if err != nil {
		t.Fatalf("idemConstraint(%q): %v", key, err)
	}
	return c
}

// A key is submitter-supplied text that ends up inside an expression the server parses.
// Each of these keys, interpolated unquoted, would change the expression's shape -- the
// first two into something that matches every record, which is the dangerous direction: a
// lookup that matches everything reports the key as taken and refuses every submission,
// and one crafted the other way reports absent and defeats the check.
func TestIdemConstraintDoesNotLetTheKeyEscape(t *testing.T) {
	for _, key := range []string{
		`x" || true || "y`,
		`" || ClusterId > 0 || "`,
		`x" =?= "x`,
		"with\\backslash",
		"with\nnewline",
		`with"quote`,
		`}] foo [{`,
	} {
		// The constraint must still parse, and must not match an ad holding a different key.
		if matches(t, key, "some-other-key") {
			t.Errorf("key %q matched an ad that does not carry it", key)
		}
		// And it must match an ad that really does carry exactly that key, so the quoting
		// round-trips rather than merely failing safe.
		if !matches(t, key, key) {
			t.Errorf("key %q did not match an ad carrying it verbatim", key)
		}
	}
}

// The attribute name has no quoted form, so it is validated rather than escaped.
func TestIdemConstraintRejectsUnusableAttrNames(t *testing.T) {
	for _, attr := range []string{
		"", "Idempotency Key", "Idempotency.Key", "0Key", "Key || true",
		`Key"`, "Key;", "MY.Key", "-Key",
	} {
		if _, err := idemConstraint(attr, "k"); err == nil {
			t.Errorf("idemConstraint(%q, ...) was accepted", attr)
		}
	}
	for _, attr := range []string{"IdempotencyKey", "_k", "K0", "a_B_9"} {
		if _, err := idemConstraint(attr, "k"); err != nil {
			t.Errorf("idemConstraint(%q, ...) was refused: %v", attr, err)
		}
	}
}

func TestIdemConstraintBoundsTheKey(t *testing.T) {
	if _, err := idemConstraint("IdempotencyKey", ""); err == nil {
		t.Error("an empty key was accepted")
	}
	if _, err := idemConstraint("IdempotencyKey", strings.Repeat("k", idemMaxKeyLen)); err != nil {
		t.Errorf("a key at the limit was refused: %v", err)
	}
	if _, err := idemConstraint("IdempotencyKey", strings.Repeat("k", idemMaxKeyLen+1)); err == nil {
		t.Error("a key over the limit was accepted")
	}
}

// idemJobID names the job that holds the key, and degrades rather than failing: a row that
// matched is a match whether or not its identity can be read out of it.
func TestIdemJobID(t *testing.T) {
	for _, tc := range []struct{ name, text, want string }{
		{"proc row", "ClusterId = 42\nProcId = 7\n", "42.7"},
		{"proc zero", "ClusterId = 42\nProcId = 0\n", "42.0"},
		{"cluster only", "ClusterId = 42\n", "42"},
		{"no cluster", "ProcId = 7\n", "?"},
		{"unparsable", "this is not an ad {{{", "?"},
		{"empty", "", "?"},
	} {
		if got := idemJobID(tc.text); got != tc.want {
			t.Errorf("%s: idemJobID(%q) = %q, want %q", tc.name, tc.text, got, tc.want)
		}
	}
}

// The archive half is bounded only when a window is asked for, and the bound keeps the key
// comparison intact: parenthesised, so the && cannot bind to part of the key's comparison.
func TestIdemHistoryBound(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cons := mustConstraint(t, "k")

	for _, w := range []int64{0, -1, -3600} {
		if got := idemHistoryBound(cons, w, now); got != cons {
			t.Errorf("window %d bounded the lookup: %q", w, got)
		}
	}

	got := idemHistoryBound(cons, 3600, now)
	want := `(IdempotencyKey =?= "k") && EnteredHistoryTime >= 1799996400`
	if got != want {
		t.Errorf("bounded constraint = %q, want %q", got, want)
	}

	// It must still be one parseable expression, and it must still select on the key:
	// a record inside the window but holding another key does not match.
	expr, perr := classad.ParseExpr(got)
	if perr != nil {
		t.Fatalf("parsing %q: %v", got, perr)
	}
	for _, tc := range []struct {
		name   string
		key    string
		stamp  int64
		expect bool
	}{
		{"in window, right key", "k", 1_799_999_000, true},
		{"in window, wrong key", "other", 1_799_999_000, false},
		{"before window, right key", "k", 1_700_000_000, false},
	} {
		ad := classad.New()
		ad.InsertAttrString("IdempotencyKey", tc.key)
		ad.InsertAttr(attrEnteredHistoryTime, tc.stamp)
		v := ad.EvaluateExprWithTarget(expr, nil)
		b, berr := v.BoolValue()
		if berr != nil {
			t.Fatalf("%s: non-boolean result: %v", tc.name, berr)
		}
		if b != tc.expect {
			t.Errorf("%s: matched = %v, want %v", tc.name, b, tc.expect)
		}
	}

	// A window longer than the epoch clamps instead of going negative.
	if got := idemHistoryBound(cons, now.Unix()+10_000, now); !strings.Contains(got, ">= 0") {
		t.Errorf("an over-long window did not clamp to 0: %q", got)
	}
}
