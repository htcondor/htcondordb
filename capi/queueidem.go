// The submission-idempotency lookup: "has any job this database knows about ever
// carried this key?"
//
// This is the one entry point in the queue family that BLOCKS, and the exception is
// deliberate. Everything in queue.go is shaped around not stalling a schedd's event
// loop while a query streams megabytes of ads. This answers a different question, on
// a different path:
//
//   - It is on the SUBMIT path, where the schedd is already blocked. A submit runs
//     inside one QMGMT connection that the schedd services synchronously, round trip
//     by round trip with condor_submit, and commits by fsyncing the job queue log.
//     One indexed point lookup is not what makes that path slow.
//   - Its answer gates a decision that cannot be deferred: either the transaction
//     commits or it is refused. There is nothing useful for the caller to do while
//     waiting, and no way to hand it a partial result.
//   - It returns one row at most, so the streaming machinery buys nothing.
//
// The caller bounds the wait and decides what an indeterminate answer means. This
// library never guesses: a lookup it could not complete is an error, never "absent".
// Reporting absence it has not established would turn the key's guarantee into a
// coin flip exactly when the database is unwell.
package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/htcondor/htcondordb/scheddsync"
)

// What hcdb_queue_lookup_key returns, alongside hcdbErr for an answer it could not
// establish. Absent and present are distinct from the error code because the caller
// treats them as opposites and the error as neither.
const (
	hcdbIdemAbsent  = 0 // nothing in the queue or the history carries the key
	hcdbIdemPresent = 1 // something does, and *found names it
)

// idemLookupDefaultTimeout bounds a lookup whose caller did not. Generous next to the
// query itself -- an indexed equality on one attribute -- because what it really has
// to cover is a connection that has gone away without the supervisor noticing yet.
const idemLookupDefaultTimeout = 10 * time.Second

// idemMaxKeyLen caps the key a caller may ask about. A key is a submitter-supplied
// string arriving from the far side of two trust boundaries, and nothing here benefits
// from an unbounded one: it would be quoted into a constraint expression that the
// server then parses. The limit is far above any sane key and far below anything that
// strains a parser.
const idemMaxKeyLen = 1024

// hcdb_queue_lookup_key reports whether any job this database holds carries key in
// attribute attr -- the live queue first, then the history archive named by
// history_table.
//
// Returns HCDB_IDEM_ABSENT, HCDB_IDEM_PRESENT with *found set, or HCDB_IDEM_ERROR with
// *err set. On HCDB_IDEM_PRESENT, *found is "queue <cluster>.<proc>" or
// "history <cluster>.<proc>" -- two tokens, so the caller composes its own message
// rather than relaying a sentence from here. Both strings are the caller's to free
// with hcdb_free.
//
// timeout_ms bounds the whole lookup, both queries together; <= 0 takes the default.
//
// history_window_seconds, when positive, restricts the archive half to jobs that entered
// the history within that many seconds. It is a performance lever with a semantic price,
// and the caller sets the price: see idemHistoryBound.
//
//export hcdb_queue_lookup_key
func hcdb_queue_lookup_key(h C.uintptr_t, attr, key, history_table *C.char, history_window_seconds C.longlong, timeout_ms C.int, found, err **C.char) C.int {
	return guardStatus("hcdb_queue_lookup_key", err, func() C.int {
		return hcdb_queue_lookup_keyImpl(h, attr, key, history_table, history_window_seconds, timeout_ms, found, err)
	})
}

func hcdb_queue_lookup_keyImpl(h C.uintptr_t, attr, key, history_table *C.char, history_window_seconds C.longlong, timeout_ms C.int, found, err **C.char) C.int {
	*found = nil
	*err = nil

	q := handleQueue(h)
	if q == nil {
		*err = C.CString("invalid queue handle")
		return hcdbErr
	}

	attrName := C.GoString(attr)
	keyText := C.GoString(key)
	histTable := C.GoString(history_table)

	cons, cerr := idemConstraint(attrName, keyText)
	if cerr != nil {
		*err = C.CString(cerr.Error())
		return hcdbErr
	}
	// An empty history table is a configuration error and not a licence to skip the
	// archive. Half a lookup that reports "absent" is the one answer this must never
	// invent: the whole reason to ask the database at all is the jobs the schedd no
	// longer has.
	if strings.TrimSpace(histTable) == "" {
		*err = C.CString("no history table was named, so the archive half of the lookup cannot be made")
		return hcdbErr
	}

	pc, release := q.acquire()
	if pc == nil {
		*err = C.CString("no session to the database")
		return hcdbErr
	}
	defer release()

	timeout := idemLookupDefaultTimeout
	if timeout_ms > 0 {
		timeout = time.Duration(timeout_ms) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(pc.c.ctx, timeout)
	defer cancel()

	// The live queue first. Its rows carry their cluster's attributes (schedd-sync
	// chains a proc ad to its cluster ad), so a key declared on a cluster ad is found
	// here on the cluster's procs without the clusters table entering into it.
	rows, qerr := pc.c.dbc.QueryRawProject(ctx, q.table, cons, []string{attrClusterId, attrProcId}, 1)
	if qerr != nil {
		*err = C.CString(fmt.Sprintf("looking up %s in %s: %v", attrName, q.table, qerr))
		return hcdbErr
	}
	if len(rows) > 0 {
		// A projected row comes back in old-ClassAd form.
		*found = C.CString("queue " + idemJobID(rows[0], true))
		return hcdbIdemPresent
	}

	// Then the history. Archives do not answer the ordinary query op at all, so this
	// is a different call rather than the same one against another table name.
	hrows, herr := pc.c.dbc.ArchiveQuery(ctx, histTable, idemHistoryBound(cons, int64(history_window_seconds), time.Now()), 1)
	if herr != nil {
		*err = C.CString(fmt.Sprintf("looking up %s in %s: %v", attrName, histTable, herr))
		return hcdbErr
	}
	if len(hrows) > 0 {
		// An archive row comes back in new-ClassAd form, unlike the projected row
		// above. The two query ops serialize differently, and reading one with the
		// other's parser yields no attributes and no error worth the name.
		*found = C.CString("history " + idemJobID(hrows[0], false))
		return hcdbIdemPresent
	}

	return hcdbIdemAbsent
}

const (
	attrClusterId          = "ClusterId"
	attrProcId             = "ProcId"
	attrEnteredHistoryTime = "EnteredHistoryTime"
)

// idemHistoryBound adds a lower bound on EnteredHistoryTime to the archive half of the
// lookup when the caller asked for one. A window of 0 or less returns cons untouched,
// which is the unbounded lookup: every record the archive holds.
//
// Why the bound exists. An equality probe on an indexed attribute is resolved from each
// segment's sidecar -- but only for the segments that have one. An index added to an
// existing archive is carried back only as far as the backfill horizon, and every segment
// sealed before that keeps the sidecar it was sealed with, so a probe on this attribute
// SCANS those segments in full. On a history table that predates the feature that is the
// whole archive, once per submit carrying a key.
//
// EnteredHistoryTime is the right attribute to bound with, and the reason is exactly why
// it is not in the index set: htcondordb stamps it at ingest, so it rises with append
// order and its zone map resolves a range to precisely the segments holding it. A lower
// bound therefore prunes older segments at the zone map, before the missing sidecar
// matters -- no index required, and nothing is read from the pruned segments at all.
//
// The semantic price is real and belongs to the caller: inside the window the key is
// enforced against finished jobs, outside it the key may be reused. One window is free of
// that price -- one reaching back to before any job could have carried a key, since no
// record sealed before the attribute existed can match. A caller that sets the window
// from when it turned the feature on buys the pruning and gives up nothing.
func idemHistoryBound(cons string, windowSeconds int64, now time.Time) string {
	if windowSeconds <= 0 {
		return cons
	}
	floor := now.Unix() - windowSeconds
	if floor < 0 {
		floor = 0
	}
	return fmt.Sprintf("(%s) && %s >= %d", cons, attrEnteredHistoryTime, floor)
}

// idemConstraint validates the caller's text and hands it to the shared expression
// builder. This is the only place a caller's text reaches an expression the server parses.
//
// The key is quoted there as a ClassAd string literal, so a key containing a quote, a
// backslash or a newline compares as those characters rather than closing the literal and
// continuing the expression. The attribute NAME cannot be quoted -- it has to appear as an
// identifier -- so it is validated here instead, against the identifier shape rather than
// against a blocklist.
//
// Why the expression has two comparisons, and why it is built in scheddsync rather than
// spelled out here, is in scheddsync.IdempotencyConstraint. The short of it: =?= is the
// comparison this wants and the one the planner will not serve from an index.
func idemConstraint(attr, key string) (string, error) {
	if !isPlainAttrName(attr) {
		return "", fmt.Errorf("%q is not a usable attribute name", attr)
	}
	if key == "" {
		return "", fmt.Errorf("the key is empty")
	}
	if len(key) > idemMaxKeyLen {
		return "", fmt.Errorf("the key is %d bytes, over the %d-byte limit", len(key), idemMaxKeyLen)
	}
	return scheddsync.IdempotencyConstraint(attr, key), nil
}

// isPlainAttrName reports whether name is an unadorned ClassAd attribute name: a letter
// or underscore, then letters, digits and underscores. Anything else -- a dot, a space,
// an operator, an empty string -- is refused rather than escaped, because an attribute
// reference has no quoted form to escape it into.
func isPlainAttrName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// idemJobID renders the "<cluster>.<proc>" of a matched row, or "?" when the row cannot
// be read as one.
//
// oldFormat picks the parser, and the caller has to know which: a projected row from the
// ordinary query op arrives as old-ClassAd "Attr = Value" lines, while an archive row
// arrives as a new-ClassAd "[ Attr = Value; ... ]". Each parser reads the other's output
// as an ad with nothing in it, so getting this wrong does not raise an error -- it just
// renders every job as "?".
//
// A row that matched but will not parse is still a match: the key IS in use, and the
// caller's decision does not depend on which job holds it. So this never fails the
// lookup -- it degrades the identification, which is a detail of the message, and
// leaves the verdict alone.
func idemJobID(text string, oldFormat bool) string {
	parse := classad.Parse
	if oldFormat {
		parse = classad.ParseOld
	}
	ad, perr := parse(text)
	if perr != nil || ad == nil {
		return "?"
	}
	cluster, cok := ad.EvaluateAttrInt(attrClusterId)
	proc, pok := ad.EvaluateAttrInt(attrProcId)
	if !cok {
		return "?"
	}
	if !pok {
		// A cluster ad, or a record whose ProcId did not survive. The cluster alone
		// still names the submission, which is what a key identifies.
		return fmt.Sprintf("%d", cluster)
	}
	return fmt.Sprintf("%d.%d", cluster, proc)
}
