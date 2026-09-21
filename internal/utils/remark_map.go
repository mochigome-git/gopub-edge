package utils

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"

	"gopub-edge/config"
	"gopub-edge/internal/session"
)

// --------------------------------------------------------------------------
// Remark label sets, selected by REMARK_MAPPING_TENANT in .env
//   gim → English
//   gcl → Japanese
// --------------------------------------------------------------------------

var remarkMaps = map[string]map[int]string{
	"gim": {
		0:  "NORMAL",
		1:  "OVERLOAD",
		2:  "PUNCHING MISS/ NO BALL",
		3:  "OVERFLOW",
		4:  "DAMAGE",
		5:  "LOW: BURETTE",
		6:  "LOW: SETTING",
		7:  "LOW: SUCTION",
		8:  "LOW: FILLING",
		9:  "HIGH: BURETTE",
		10: "HIGH: SETTING",
		11: "HIGH: SUCTION",
		12: "HIGH: FILLING",
		13: "LEAKING",
		14: "BURETTE ISSUE",
		15: "BUBBLE",
		16: "NO INK",
		27: "PRINT TEST",
	},
	"gcl": {
		0:  "良好",
		1:  "充填量下限割れ",
		2:  "充填量上限オーバー",
		3:  "ノズル詰まり",
		4:  "玉打ちNG",
		5:  "カートリッジ傷・凹み等",
		6:  "落下",
		7:  "作業操作ミス",
		8:  "吸引NG",
		9:  "機械トラブル",
		10: "真空NG",
		11: "設定ミス",
		12: "印字NG",
		13: "その他",
		14: "なし",
		15: "なし",
		16: "なし",
		27: "印字テスト",
	},
}

const defaultRemarkTenant = "gim"

var (
	activeRemarkMap map[int]string
	remarkMapOnce   sync.Once
)

// RemarkStatusMap returns the label set selected by REMARK_MAPPING_TENANT.
// Resolved once; unknown/empty values fall back to "gim" with a warning.
func RemarkStatusMap() map[int]string {
	remarkMapOnce.Do(func() {
		tenant := strings.ToLower(strings.TrimSpace(config.RemarkMappingTenant))
		m, ok := remarkMaps[tenant]
		if !ok {
			log.Printf("⚠ REMARK_MAPPING_TENANT=%q unknown, falling back to %q", tenant, defaultRemarkTenant)
			tenant = defaultRemarkTenant
			m = remarkMaps[tenant]
		}
		log.Printf("[Remark] using %q remark labels", tenant)
		activeRemarkMap = m
	})
	return activeRemarkMap
}

// IsNormalRemark reports whether s is the code-0 ("good") label of any
// tenant, so the fault latch and good/reject counting work for both
// "NORMAL" and "良好".
func IsNormalRemark(s string) bool {
	s = strings.TrimSpace(s)
	for _, m := range remarkMaps {
		if strings.EqualFold(s, m[0]) {
			return true
		}
	}
	return false
}

// remarkToInt converts a PLC register value of any common JSON shape to int.
func remarkToInt(value any) (int, bool) {
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
	statusMap := RemarkStatusMap()
	normal := statusMap[0]

	lookupRemark := func(key string) string {
		value, exists := jsonPayloads.GetAny(key)
		if !exists || value == nil {
			return normal
		}
		code, ok := remarkToInt(value)
		if !ok {
			log.Printf("[Remark] %s: unparseable value %v (%T) → %s", key, value, value, normal)
			return normal
		}
		label, found := statusMap[code]
		if !found {
			log.Printf("[Remark] %s: code %d out of range → %s", key, code, normal)
			return normal
		}
		return label
	}

	remarks := map[string]string{
		"ch1_remark": lookupRemark("d26"),
		"ch2_remark": lookupRemark("d27"),
		"ch3_remark": lookupRemark("d28"),
	}

	// Latch faults: once a channel shows a non-normal remark, don't let a
	// later normal overwrite it before the patch fires. processPatch clears
	// ProcessedPayloadsMap after each patch, which resets the latch.
	session.Mutex.Lock()
	for key, val := range remarks {
		prev := ""
		if existing, ok := session.ProcessedPayloadsMap[key]; ok {
			if p, hasPrev := existing[key].(string); hasPrev {
				prev = p
			}
		}

		if prev != "" && !IsNormalRemark(prev) && IsNormalRemark(val) {
			// incoming normal against a latched fault — count it
			session.RemarkNormalStreak[key]++
			if session.RemarkNormalStreak[key] < NormalConfirmCycles {
				continue // not confirmed yet, keep the fault latched
			}
			// confirmed sustained normal → operator cleared it, allow overwrite
		} else {
			// any fault (or first write) resets the streak
			session.RemarkNormalStreak[key] = 0
		}

		session.ProcessedPayloadsMap[key] = map[string]any{key: val}
	}
	session.Mutex.Unlock()

	jsonPayloads.Delete("d26")
	jsonPayloads.Delete("d27")
	jsonPayloads.Delete("d28")
}
