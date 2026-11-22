package scheddsync

import (
	"github.com/PelicanPlatform/classad/ast"
)

// IdempotencyKeyAttr is the job attribute carrying a submission's idempotency key. It is
// set on the cluster ad, so chaining puts it on every proc row of the live queue and
// sPrintAd flattens it into every history record -- one attribute name covers both
// tables, and a lookup need not know whether a cluster or a proc declared it.
const IdempotencyKeyAttr = "IdempotencyKey"

/*
 * IdempotencyConstraint is the expression that finds the job holding key, and it is two
 * terms where one would read more naturally:
 *
 *	IdempotencyKey == "<key>" && IdempotencyKey =?= "<key>"
 *
 * =?= alone is what the lookup wants -- ClassAd's == folds case on strings, and a caller
 * who chose "Batch-7" has not used up "batch-7". But the planner serves a categorical
 * index for == and NOT for =?=, measured: on a 600-record archive indexed on this
 * attribute, `=?= ` plans as a serial scan and `==` plans as indexed. So =?= on its own
 * reads every record in the history on every submission that carries a key, and the index
 * the table pays for contributes nothing.
 *
 * Both terms together get both properties. The folded == narrows through the index to the
 * case-variants of the key, normally exactly one record; the =?= then decides, so the
 * answer is case-sensitive exactly as before. A site holding both "Batch-7" and "batch-7"
 * costs one extra record to filter.
 *
 * This lives here, beside the attribute name, because the index set and the test that the
 * probe is actually indexed are in a third package. A copy of the expression anywhere else
 * could drift from the one that ships while every test went on passing -- which is the
 * failure this shape already had once.
 */
func IdempotencyConstraint(attr, key string) string {
	quoted := ast.QuoteString(key)
	return attr + " == " + quoted + " && " + attr + " =?= " + quoted
}
