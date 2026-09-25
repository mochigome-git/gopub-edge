package utils

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"

	"gopub-edge/internal/session"
)

// Remarks are sent as raw PLC codes (d26/d27/d28 → ch1/ch2/ch3_remark).
// Code 0 = good; anything else is a reject reason. The code → label
// translation (per tenant, en/ja) lives in the DB reason-code table,
// not on the edge.

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
	for reg, key := range map[string]string{"d26": "ch1_remark", "d27": "ch2_remark", "d28": "ch3_remark"} {
		if code, ok := lookup(reg); ok {
			remarks[key] = code
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
				continue
			}
		} else {
			session.RemarkNormalStreak[key] = 0
		}
		session.ProcessedPayloadsMap[key] = map[string]any{key: val}
	}
	session.Mutex.Unlock()

	jsonPayloads.Delete("d26")
	jsonPayloads.Delete("d27")
	jsonPayloads.Delete("d28")
}
