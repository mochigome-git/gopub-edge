package utils

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"

	"gopub-edge/internal/session"
)

// Remarks are sent as raw PLC codes (d26/d27/d28 → ch1/ch2/ch3_remark).
// Code 0 = good; anything else is a reject code. The code → label
// translation (per tenant, en/ja) lives in the DB reject-code table,
// not on the edge.
//
// Each reject can also carry a cause (原因) (d57/d58/d59 → ch1/ch2/ch3_cause),
// a second raw code the operator / PLC picks (1, 2, 3, …) that explains
// the reject. Its labels live in the DB reject-cause table. A cause is
// only sent alongside a non-zero remark and is latched with it: it stays
// as long as the remark stays, and goes away when the remark clears.

//"gim":
//	0:  "NORMAL",
//	1:  "OVERLOAD",
//	2:  "PUNCHING MISS/ NO BALL",
//	3:  "OVERFLOW",
//	4:  "DAMAGE",
//	5:  "LOW: BURETTE",
//	6:  "LOW: SETTING",
//	7:  "LOW: SUCTION",
//	8:  "LOW: FILLING",
//	9:  "HIGH: BURETTE",
//	10: "HIGH: SETTING",
//	11: "HIGH: SUCTION",
//	12: "HIGH: FILLING",
//	13: "LEAKING",
//	14: "BURETTE ISSUE",
//	15: "BUBBLE",
//	16: "NO INK",
//	27: "PRINT TEST",
//
//"gcl":
//	0:  "良好",
//	1:  "充填量下限割れ",
//	2:  "充填量上限オーバー",
//	3:  "ノズル詰まり",
//	4:  "玉打ちNG",
//	5:  "カートリッジ傷・凹み等",
//	6:  "落下",
//	7:  "作業操作ミス",
//	8:  "吸引NG",
//	9:  "機械トラブル",
//	10: "真空NG",
//	11: "設定ミス",
//	12: "印字NG",
//	13: "その他",
//	14: "なし",
//	15: "なし",
//	16: "なし",
//	27: "印字テスト",

// RemarkToInt converts a PLC register value of any common JSON shape to int.
func RemarkToInt(value any) (int, bool) {
	switch v := value.(type) {
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	case int:
		return v, true
	case int64:
		return int(v), true
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return int(n), true
		}
		if f, err := v.Float64(); err == nil {
			return int(f), true
		}
		return 0, false
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int(f), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// NormalConfirmCycles is how many consecutive normal cycles are needed to
// clear a latched fault.
const NormalConfirmCycles = 5

func RemarkMapping(jsonPayloads *SafeJsonPayloads, session *session.Session) {
	lookup := func(key string) (int, bool) {
		v, ok := jsonPayloads.GetAny(key)
		if !ok || v == nil {
			return 0, false // register absent → don't emit this channel
		}
		code, ok := RemarkToInt(v)
		if !ok {
			log.Printf("[Remark] %s: unparseable %v (%T), skipped", key, v, v)
			return 0, false
		}
		return code, true
	}

	remarks := map[string]int{}
	for reg, key := range remarkRegisters {
		if code, ok := lookup(reg); ok {
			remarks[key] = code
		}
	}
	causes := map[string]int{}
	for reg, key := range causeRegisters {
		if code, ok := lookup(reg); ok {
			causes[key] = code
		}
	}

	// Latch faults: once a channel shows a non-zero code, keep it until
	// NormalConfirmCycles consecutive zeros arrive (or the patch clears it).
	session.Mutex.Lock()
	for key, val := range remarks {
		prev := -1
		if existing, ok := session.ProcessedPayloadsMap[key]; ok {
			if p, ok := RemarkToInt(existing[key]); ok {
				prev = p
			}
		}
		if prev > 0 && val == 0 {
			session.RemarkNormalStreak[key]++
			if session.RemarkNormalStreak[key] < NormalConfirmCycles {
				continue // remark (and its cause) stay latched
			}
		} else {
			session.RemarkNormalStreak[key] = 0
		}
		session.ProcessedPayloadsMap[key] = map[string]any{key: val}
		latchCause(session, remarkToCause[key], val, causes)
	}
	session.Mutex.Unlock()

	for reg := range remarkRegisters {
		jsonPayloads.Delete(reg)
	}
	for reg := range causeRegisters {
		jsonPayloads.Delete(reg)
	}
}

var remarkRegisters = map[string]string{"d26": "ch1_remark", "d27": "ch2_remark", "d28": "ch3_remark"}

var causeRegisters = map[string]string{"d57": "ch1_cause", "d58": "ch2_cause", "d59": "ch3_cause"}

var remarkToCause = map[string]string{"ch1_remark": "ch1_cause", "ch2_remark": "ch2_cause", "ch3_remark": "ch3_cause"}

// latchCause keeps a channel's cause in step with its (just stored)
// remark. Caller holds session.Mutex.
//   - remark 0 (good, confirmed): no cause — drop any held one.
//   - remark non-zero: take the live cause when it's non-zero; if the PLC
//     shows 0 / nothing this cycle (cause not picked yet, or already
//     reset), keep the one already held so the reject keeps its cause.
func latchCause(session *session.Session, causeKey string, remark int, causes map[string]int) {
	if remark == 0 {
		delete(session.ProcessedPayloadsMap, causeKey)
		return
	}
	if r, ok := causes[causeKey]; ok && r != 0 {
		session.ProcessedPayloadsMap[causeKey] = map[string]any{causeKey: r}
	}
}
