// utils/multi_register.go

package utils

import (
	"os"
	"strconv"
	"strings"
)

// MultiRegisterSpec describes one <prefix><TYPE>_<KEY>_RE<n> env group.
type MultiRegisterSpec struct {
	OutputKey string // e.g. "operator", "model_name"
	EnvPrefix string // e.g. "CASE_JOB_STR_OPERATOR_RE" (trailing "_RE" kept)
	MaxIdx    int    // highest RE index found -> register count = MaxIdx+1
}

// DiscoverMultiRegisterSpecs scans os.Environ() for every
// <prefix><TYPE>_<KEY>_RE<n> group and returns one spec per group,
// with OutputKey derived by stripping the TYPE marker (STR_/LI_/MN_)
// and lowercasing. Adding a field is then purely a .env change — no
// Go code needs to know the register count.
func DiscoverMultiRegisterSpecs(prefix string, typeMarkers []string) []MultiRegisterSpec {
	groups := map[string]*MultiRegisterSpec{}

	for _, env := range os.Environ() {
		name, _, ok := strings.Cut(env, "=")
		if !ok || !strings.HasPrefix(name, prefix) {
			continue
		}
		reIdx := strings.LastIndex(name, "_RE")
		if reIdx < 0 {
			continue
		}
		idx, err := strconv.Atoi(name[reIdx+3:])
		if err != nil {
			continue
		}

		envPrefix := name[:reIdx+3]
		raw := name[len(prefix):reIdx]
		outputKey := strings.ToLower(raw)
		for _, marker := range typeMarkers {
			outputKey = strings.TrimPrefix(outputKey, strings.ToLower(marker)+"_")
		}

		g, ok := groups[envPrefix]
		if !ok {
			g = &MultiRegisterSpec{OutputKey: outputKey, EnvPrefix: envPrefix}
			groups[envPrefix] = g
		}
		if idx > g.MaxIdx {
			g.MaxIdx = idx
		}
	}

	specs := make([]MultiRegisterSpec, 0, len(groups))
	for _, g := range groups {
		specs = append(specs, *g)
	}
	return specs
}

// ReadDiscoveredMultiRegisterFields runs DiscoverMultiRegisterSpecs then
// decodes each with ReadMultiRegisterString. maxLenOverrides lets a
// caller trim specific fields (see ConvertAndStoreModelName's
// PRODUCT_NAME comment: 15 digits packed into 8 registers/16 bytes).
// Returns decoded values plus every device key consumed, so callers
// that need to delete used keys (case10) can, while callers that
// don't (job_case) just ignore the second return.
func ReadDiscoveredMultiRegisterFields(
	jsonPayloads *SafeJsonPayloads,
	prefix string,
	typeMarkers []string,
	maxLenOverrides map[string]int,
) (map[string]string, []string) {
	result := make(map[string]string)
	var allUsedKeys []string

	for _, spec := range DiscoverMultiRegisterSpecs(prefix, typeMarkers) {
		length := maxLenOverrides[spec.OutputKey]
		val, usedKeys := ReadMultiRegisterString(jsonPayloads, spec.EnvPrefix, spec.MaxIdx, length)
		if val != "" {
			result[spec.OutputKey] = val
			allUsedKeys = append(allUsedKeys, usedKeys...)
		}
	}
	return result, allUsedKeys
}
