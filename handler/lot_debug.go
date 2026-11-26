// lot_debug.go
package handler

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopub-edge/internal/session"
	"gopub-edge/model"
)

// LOT_DEBUG=true prints one line per batch from the lot handler: what it
// received for the trigger, the reset counter and every condition, and
// what it decided. For troubleshooting only — it logs several lines a second.
func lotDebugOn() bool { return os.Getenv("LOT_DEBUG") == "true" }

// runLength compresses a reading sequence: [0 0 0 1 1 0] -> "0×3 1×2 0×1".
func runLength(vals []bool) string {
	if len(vals) == 0 {
		return "-"
	}
	var b strings.Builder
	cur, n := vals[0], 0
	flush := func() {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d×%d", map[bool]int{false: 0, true: 1}[cur], n)
	}
	for _, v := range vals {
		if v != cur {
			flush()
			cur, n = v, 0
		}
		n++
	}
	flush()
	return b.String()
}

// batchAddresses lists the distinct addresses in a batch (max 12), to show
// what actually arrived when the trigger is missing.
func batchAddresses(messages []model.Message) string {
	seen := map[string]bool{}
	for _, m := range messages {
		seen[m.Address] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > 12 {
		keys = append(keys[:12], fmt.Sprintf("…+%d", len(keys)-12))
	}
	return strings.Join(keys, ",")
}

func registerSpan(reg string, messages []model.Message) string {
	var vals []float64
	for _, m := range messages {
		if strings.EqualFold(m.Address, reg) {
			if v, ok := readNumber(m.Value); ok {
				vals = append(vals, v)
			}
		}
	}
	if len(vals) == 0 {
		return reg + " none"
	}
	lo, hi := vals[0], vals[0]
	for _, v := range vals {
		lo, hi = min(lo, v), max(hi, v)
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	return fmt.Sprintf("%s n=%d first=%s last=%s min=%s max=%s", reg, len(vals), f(vals[0]), f(vals[len(vals)-1]), f(lo), f(hi))
}

func logLotDebug(
	sess *session.Session,
	triggerKey string,
	messages []model.Message,
	values []bool,
	prevBefore bool,
	buttonEdge, resetEdge bool,
	conds []lotCondition,
) {
	if !lotDebugOn() {
		return
	}
	var parts []string
	parts = append(parts, fmt.Sprintf("batch %d msgs", len(messages)))
	if len(values) == 0 {
		parts = append(parts, fmt.Sprintf("%s NOT in batch (got: %s)", triggerKey, batchAddresses(messages)))
	} else {
		sess.Mutex.Lock()
		prevAfter := sess.LotTriggerPrev
		sess.Mutex.Unlock()
		parts = append(parts, fmt.Sprintf("%s n=%d seq=[%s] prev %v->%v edge=%v",
			triggerKey, len(values), runLength(values), prevBefore, prevAfter, buttonEdge))
	}
	if reg := strings.ToLower(strings.TrimSpace(os.Getenv(lotResetRegisterEnv))); reg != "" {
		parts = append(parts, fmt.Sprintf("%s reset=%v", registerSpan(reg, messages), resetEdge))
	}
	if len(conds) > 0 {
		sess.Mutex.Lock()
		var cs []string
		for _, c := range conds {
			last, seen := sess.LotCondLast[c.Register]
			cs = append(cs, fmt.Sprintf("%s met=%v last=%s", c, sess.LotCondMet[c.String()], fmtNum(last, seen)))
		}
		sess.Mutex.Unlock()
		parts = append(parts, "conds: "+strings.Join(cs, "; "))
	}
	sess.Mutex.Lock()
	if !sess.LotLastCreated.IsZero() {
		parts = append(parts, fmt.Sprintf("last lot %s ago", time.Since(sess.LotLastCreated).Round(time.Second)))
	}
	sess.Mutex.Unlock()
	log.Printf("[lot-debug] %s", strings.Join(parts, " | "))
}
