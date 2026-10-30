// utils/register_int64.go

package utils

import (
	"fmt"
	"os"
	"strconv"
)

// int64TypeMarker marks a 4-register signed 64-bit integer field, e.g.
// a GT Designer 数値入力 object with data format 符号付きBIN64:
//
//	CASE_JOB_I64_PRODUCT_CODE_RE0=d2300   # lowest word
//	CASE_JOB_I64_PRODUCT_CODE_RE1=d2301
//	CASE_JOB_I64_PRODUCT_CODE_RE2=d2302
//	CASE_JOB_I64_PRODUCT_CODE_RE3=d2303   # highest word
//
// The registers must be read as plain 16-bit words (the *_DEVICES_16bit
// list), not ASCII — decoding a BIN64 as text produces garbage.
const int64TypeMarker = "I64"

// ReadDiscoveredInt64Fields decodes every <prefix>I64_<KEY>_RE0..RE3 group
// into a decimal string under the lowercased <key>. A group whose
// registers aren't all present in this payload is skipped, so a
// half-read value is never stored. Returns the device keys consumed.
func ReadDiscoveredInt64Fields(jsonPayloads *SafeJsonPayloads, prefix string) (map[string]string, []string) {
	result := make(map[string]string)
	var allUsedKeys []string

	for _, spec := range DiscoverMultiRegisterSpecs(prefix, []string{int64TypeMarker}) {
		val, usedKeys, ok := readMultiRegisterInt64(jsonPayloads, spec.EnvPrefix)
		allUsedKeys = append(allUsedKeys, usedKeys...)
		if ok {
			result[spec.OutputKey] = strconv.FormatInt(val, 10)
		}
	}
	return result, allUsedKeys
}

// readMultiRegisterInt64 combines RE0..RE3 (Mitsubishi order: lowest
// address = least significant word) into one signed 64-bit value.
func readMultiRegisterInt64(jsonPayloads *SafeJsonPayloads, keyPrefix string) (int64, []string, bool) {
	var u uint64
	var usedKeys []string

	for i := 0; i < 4; i++ {
		deviceKey := os.Getenv(fmt.Sprintf("%s%d", keyPrefix, i))
		if deviceKey == "" {
			return 0, usedKeys, false
		}
		raw, exists := jsonPayloads.Get(deviceKey)
		if !exists {
			return 0, usedKeys, false
		}
		usedKeys = append(usedKeys, deviceKey)
		word, ok := registerWord(raw)
		if !ok {
			return 0, usedKeys, false
		}
		u |= uint64(word) << (16 * i)
	}
	return int64(u), usedKeys, true
}

// registerWord normalises one 16-bit register value, whether the reader
// published it signed (-32768..32767) or unsigned (0..65535).
func registerWord(v any) (uint16, bool) {
	var n int64
	switch x := v.(type) {
	case float64:
		n = int64(x)
	case int:
		n = int64(x)
	case int64:
		n = x
	case string:
		parsed, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, false
		}
		n = parsed
	default:
		return 0, false
	}
	return uint16(n & 0xFFFF), true
}
