package utils

import (
	"fmt"
	"os"
	"strings"
)

// ReadMultiRegisterString reads count+1 consecutive PLC word registers
// (each holds 2 ASCII chars, byte-swapped — same convention as
// CalculateAndStoreInklot / ConvertAndStoreModelName), reverses each
// individually, and concatenates in register order. maxLen trims any
// trailing padding char if the register count doesn't divide evenly
// (0 = no trim).
func ReadMultiRegisterString(jsonPayloads *SafeJsonPayloads, keyPrefix string, count int, maxLen int) (string, []string) {
	var builder strings.Builder
	var usedKeys []string

	for i := 0; i <= count; i++ {
		envKey := fmt.Sprintf("%s%d", keyPrefix, i)
		deviceKey := os.Getenv(envKey)
		if deviceKey == "" {
			continue
		}
		val, ok := jsonPayloads.GetString(deviceKey)
		if !ok {
			continue
		}
		builder.WriteString(reverseString(val))
		usedKeys = append(usedKeys, deviceKey)
	}

	result := sanitizeString(builder.String())
	if maxLen > 0 && len(result) > maxLen {
		result = result[:maxLen]
	}
	return result, usedKeys
}

/**
# product_code — 10 digits → 5 registers (5 × 2 = 10, exact)
CASE_JOB_STR_PRODUCT_CODE_RE0=d500
CASE_JOB_STR_PRODUCT_CODE_RE1=d501
CASE_JOB_STR_PRODUCT_CODE_RE2=d502
CASE_JOB_STR_PRODUCT_CODE_RE3=d503
CASE_JOB_STR_PRODUCT_CODE_RE4=d504

# product_name — 15 digits → 8 registers (8 × 2 = 16, one char over → trim)
CASE_JOB_STR_PRODUCT_NAME_RE0=d510
CASE_JOB_STR_PRODUCT_NAME_RE1=d511
CASE_JOB_STR_PRODUCT_NAME_RE2=d512
CASE_JOB_STR_PRODUCT_NAME_RE3=d513
CASE_JOB_STR_PRODUCT_NAME_RE4=d514
CASE_JOB_STR_PRODUCT_NAME_RE5=d515
CASE_JOB_STR_PRODUCT_NAME_RE6=d516
CASE_JOB_STR_PRODUCT_NAME_RE7=d517

*/
