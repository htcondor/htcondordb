package server

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// A required attribute the derivation cannot make a column of is reported, with the reason, and
// reported once rather than on every pass. Without this the operator sees only the line naming
// what was required and has no way to tell a working required attribute from one that is silently
// doing nothing -- the archive reads it from whole records, which shows up only as slow queries.
func TestMaintainArchivesReportsRefusedRequireAttrs(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("history", db.ArchiveConfig{
		SegmentSize: 1 << 12,
		ValueAttrs:  []string{"ClusterId"},
		// NoSuchAttribute is in no record, so it cannot become a column. Usage is an expression
		// in every record but a computable one, which the computed-cell capability admits -- it
		// is here so the test also pins that such an attribute is NOT reported as refused.
		SchemaRequireAttrs: []string{"ClusterId", "NoSuchAttribute", "Usage"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		ad, err := classad.Parse(fmt.Sprintf(
			`[ ClusterId = %d; Owner = "u%d"; JobStatus = 4; Usage = (ClusterId * 2) ]`, i, i%10))
		if err != nil {
			t.Fatal(err)
		}
		if err := arch.Append(ad); err != nil {
			t.Fatal(err)
		}
	}

	// The derivation is what refuses an attribute, and it runs on the db maintenance loop rather
	// than this one; reporting is decoupled from it and names whatever the last derivation refused.
	if !arch.BuildAndEnableSchemaScan(4096, 8) {
		t.Fatal("schema scan did not enable")
	}

	var buf bytes.Buffer
	s := &Service{cat: cat, log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	s.maintainArchivesOnce(0)
	first := buf.String()

	// Names come back folded, so compare without case.
	lower := strings.ToLower(first)
	if !strings.Contains(lower, "nosuchattribute") {
		t.Errorf("an absent required attribute was not reported; log:\n%s", first)
	}
	if strings.Contains(lower, "usage") {
		t.Errorf("a computable expression-valued attribute was reported as refused; log:\n%s", first)
	}
	if !strings.Contains(first, "refused") {
		t.Errorf("no refusal line in the log:\n%s", first)
	}
	if !strings.Contains(first, "archive=history") {
		t.Errorf("refusal did not name the archive; log:\n%s", first)
	}

	// A standing refusal is logged once, not on every pass.
	buf.Reset()
	s.maintainArchivesOnce(0)
	s.maintainArchivesOnce(0)
	if got := buf.String(); strings.Contains(got, "refused") {
		t.Errorf("an unchanged refusal was logged again on a later pass:\n%s", got)
	}
}
