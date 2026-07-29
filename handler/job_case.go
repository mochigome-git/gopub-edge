// job_case.go
package handler

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
)

// jobFieldPrefix is the ONLY env prefix job-case fields use now — single
// register or multi-register, no more CASE_JOB_ vs CASE_JSON_JOB_ split.
// Multi-register fields are distinguished purely by having a _RE<n>
// suffix, e.g.:
//
//	CASE_JOB_room_temp=w69                  -> single register
//	CASE_JOB_STR_OPERATOR_RE0=w50 ... RE7    -> multi-register (STR)
//	CASE_JOB_LI_INK_LOT_RE0=d1120 ... RE3    -> multi-register (LI)
//	CASE_JOB_STR_DAILY_CHECK_1_RE0=w8b ...   -> multi-register, nests
//	                                             into daily_check
//
// NOTE: this means renaming the existing CASE_JSON_JOB_STR_*/LI_* env
// vars to CASE_JOB_STR_*/LI_* — see the .env diff. If you'd rather not
// touch the .env, tell me and I'll make discoverMultiRegisterSpecs scan
// both "CASE_JOB_" and "CASE_JSON_JOB_" instead of unifying them.
const jobFieldPrefix = "CASE_JOB_"

func handleJobCase(
	session *session.Session,
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
) {
	buttonVal, exists := jsonPayloads.Get(tk.TriggerKey)
	if !exists {
		return
	}
	var buttonOn bool
	switch v := buttonVal.(type) {
	case float64:
		buttonOn = v == 1
	case string:
		buttonOn = v == "1"
	case bool:
		buttonOn = v
	default:
		log.Printf("handleJobCase: trigger key %q has unexpected type %T (value %v) — ignoring message", tk.TriggerKey, buttonVal, buttonVal)
		return
	}

	session.Mutex.Lock()
	wasOn := session.JobInProgress
	session.Mutex.Unlock()

	if buttonOn && !wasOn {
		session.Mutex.Lock()
		session.JobInProgress = true
		session.JobStartedAt = time.Now().UTC()
		session.JobEndedAt = time.Time{}
		session.ProcessedPayloadsMap = make(map[string]map[string]any)
		session.Mutex.Unlock()
	}

	if buttonOn {
		dailyCheck := map[string]any{}

		// Single-register fields (no _RE suffix): total_output,
		// room_temp, appearance_1, reject_* etc.
		for k, v := range readSingleRegisterFields(jsonPayloads) {
			if isDailyCheckGridField(k) {
				dailyCheck[k] = v
				continue
			}
			if isDateTimeComponentField(k) {
				continue // combined into started_at/ended_at below, not stored raw
			}
			storeJobFieldToSession(session, k, v)
		}

		// If the PLC gives us a real start timestamp, prefer it over
		// the time.Now() capture above.
		transformations := utils.GetKeyTransformationsFromEnv(jobFieldPrefix)
		if t, ok := readDateTime(jsonPayloads, transformations, "START_DATE_TIME_"); ok {
			session.Mutex.Lock()
			session.JobStartedAt = t
			session.Mutex.Unlock()
		}

		// Try END_DATE_TIME_ on every ON-branch message too, not just
		// once at the OFF edge — the PLC may not have written
		// d148-d153 yet at the exact moment the trigger drops, but it
		// likely has by some earlier ON message, and whichever read
		// succeeds last (closest to the real end) wins here since we
		// keep overwriting session.JobEndedAt.
		if t, ok := readDateTime(jsonPayloads, transformations, "END_DATE_TIME_"); ok {
			session.Mutex.Lock()
			session.JobEndedAt = t
			session.Mutex.Unlock()
		}

		// filling_date is a plain year/month/day, not a packed multi-register
		// string — read it the same way START/END_DATE_TIME_ are read, and
		// store it in RFC3339 so it round-trips through Supabase the same as
		// started_at/ended_at.
		if t, ok := readDateOnly(jsonPayloads, transformations, "FILLING_DATE_"); ok {
			storeJobFieldToSession(session, "filling_date", t.Format(time.RFC3339))
		}

		// Multi-register text fields (_RE0.._REn): operator,
		// product_code, ink_lot, daily_check_1..4, etc. — discovered
		// from env, not hardcoded per field.
		for k, v := range readMultiRegisterFields(jsonPayloads) {
			if strings.HasPrefix(k, "daily_check_") {
				dailyCheck[k] = v
				continue
			}
			storeJobFieldToSession(session, k, v)
		}

		if len(dailyCheck) > 0 {
			storeJobFieldToSession(session, "daily_check", dailyCheck)
		}
		return
	}

	if !buttonOn && wasOn {
		session.Mutex.Lock()
		startedAt := session.JobStartedAt
		endedAt := session.JobEndedAt // last value captured during ON, may be zero
		session.JobInProgress = false
		session.Mutex.Unlock()

		// One more attempt right at the OFF edge, in case the PLC's
		// final write lands on this exact message.
		if t, ok := readDateTime(jsonPayloads, utils.GetKeyTransformationsFromEnv(jobFieldPrefix), "END_DATE_TIME_"); ok {
			endedAt = t
		}
		if endedAt.IsZero() {
			log.Printf("handleJobCase: END_DATE_TIME_* never captured during job — falling back to time.Now() for ended_at")
			endedAt = time.Now().UTC()
		}
		storeJobFieldToSession(session, "started_at", startedAt.Format(time.RFC3339))
		storeJobFieldToSession(session, "ended_at", endedAt.Format(time.RFC3339))

		session.Mutex.Lock()
		if fc, ok := session.ProcessedPayloadsMap["filling_code"]; ok && fc["filling_code"] != nil && fc["filling_code"] != "" {
			session.ProcessedPayloadsMap["job_ref"] = map[string]any{"job_ref": fc["filling_code"]}
		} else if il, ok := session.ProcessedPayloadsMap["ink_lot"]; ok {
			session.ProcessedPayloadsMap["job_ref"] = map[string]any{"job_ref": il["ink_lot"]}
		}

		// No manually maintained keys list. job_summary.go already
		// knows which of these have dedicated columns (total_output,
		// model_name, ...) and deletes them before marshalling the
		// rest into meta — so whatever we didn't explicitly handle
		// above just rides along and lands in meta automatically.
		keys := make([]string, 0, len(session.ProcessedPayloadsMap))
		for k := range session.ProcessedPayloadsMap {
			keys = append(keys, k)
		}
		session.Mutex.Unlock()

		processPatch(session, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil, true)

		session.Mutex.Lock()
		session.ProcessedPayloadsMap = make(map[string]map[string]any)
		session.Mutex.Unlock()
	}
}

// isDailyCheckGridField identifies the 12-cell appearance/balance/zero
// grid so it routes into the daily_check object instead of the flat
// top-level payload.
func isDailyCheckGridField(name string) bool {
	return strings.HasPrefix(name, "appearance_") ||
		strings.HasPrefix(name, "balance_") ||
		strings.HasPrefix(name, "zero_")
}

// readSingleRegisterFields reads every CASE_JOB_<key>=<register> entry
// that has no _RE<n> suffix — one register, one value.
func readSingleRegisterFields(jsonPayloads *utils.SafeJsonPayloads) map[string]any {
	transformations := utils.GetKeyTransformationsFromEnv(jobFieldPrefix)
	result := make(map[string]any)
	for newKey, oldKey := range transformations {
		if isMultiRegisterKey(newKey) {
			continue // handled by readMultiRegisterFields instead
		}
		if value, exists := jsonPayloads.Get(oldKey); exists {
			result[newKey] = value
		}
	}
	return result
}

// multiRegisterSpec describes one CASE_JOB_<TYPE>_<KEY>_RE<n> group.
type multiRegisterSpec struct {
	outputKey string // e.g. "operator", "daily_check_1"
	envPrefix string // e.g. "CASE_JOB_STR_OPERATOR_RE" (kept with trailing "_RE")
	maxIdx    int    // highest RE index found -> register count = maxIdx+1
}

// readMultiRegisterFields discovers every multi-register group from env
// and decodes each with ReadMultiRegisterString. Adding a new
// multi-register field now only requires adding env vars — no new Go
// code, no new hardcoded call.
func readMultiRegisterFields(jsonPayloads *utils.SafeJsonPayloads) map[string]string {
	result := make(map[string]string)
	for _, spec := range discoverMultiRegisterSpecs() {
		// maxLen=0 means "no trim" — sanitizeString already strips
		// padding, so trimming is only needed for the rare field
		// whose real digit count is odd relative to register
		// capacity (e.g. a 15-digit value packed into 8 registers /
		// 16 bytes). We don't hardcode that per field anymore; if a
		// specific field turns out to need it, add it to
		// multiRegisterMaxLenOverrides below instead of guessing.
		length := multiRegisterMaxLenOverrides[spec.outputKey]
		val, _ := utils.ReadMultiRegisterString(jsonPayloads, spec.envPrefix, spec.maxIdx, length)
		if val != "" {
			result[spec.outputKey] = val
		}
	}
	return result
}

// multiRegisterMaxLenOverrides holds the small number of fields that need
// an explicit trim because their real digit count is odd relative to
// register capacity. Empty/missing entries default to 0 (no trim).
// e.g. multiRegisterMaxLenOverrides["operator"] = 15
var multiRegisterMaxLenOverrides = map[string]int{}

func discoverMultiRegisterSpecs() []multiRegisterSpec {
	groups := map[string]*multiRegisterSpec{}

	for _, env := range os.Environ() {
		name, _, ok := strings.Cut(env, "=")
		if !ok || !strings.HasPrefix(name, jobFieldPrefix) {
			continue
		}
		reIdx := strings.LastIndex(name, "_RE")
		if reIdx < 0 {
			continue // single-register field, not ours here
		}
		idx, err := strconv.Atoi(name[reIdx+3:])
		if err != nil {
			continue
		}

		envPrefix := name[:reIdx+3] // e.g. "CASE_JOB_STR_OPERATOR_RE"
		outputKey := multiRegisterOutputKey(name[len(jobFieldPrefix):reIdx])

		g, ok := groups[envPrefix]
		if !ok {
			g = &multiRegisterSpec{outputKey: outputKey, envPrefix: envPrefix}
			groups[envPrefix] = g
		}
		if idx > g.maxIdx {
			g.maxIdx = idx
		}
	}

	specs := make([]multiRegisterSpec, 0, len(groups))
	for _, g := range groups {
		specs = append(specs, *g)
	}
	return specs
}

// multiRegisterOutputKey strips the STR_/LI_ type marker and lowercases
// the rest: "STR_PRODUCT_CODE" -> "product_code", "LI_INK_LOT" ->
// "ink_lot", "STR_DAILY_CHECK_1" -> "daily_check_1".
func multiRegisterOutputKey(raw string) string {
	raw = strings.TrimPrefix(raw, "STR_")
	raw = strings.TrimPrefix(raw, "LI_")
	return strings.ToLower(raw)
}

func isMultiRegisterKey(name string) bool {
	return strings.HasPrefix(name, "STR_") || strings.HasPrefix(name, "LI_")
}

// isDateTimeComponentField matches CASE_JOB_START_DATE_TIME_* and
// CASE_JOB_END_DATE_TIME_* — these are individual year/month/day/hour/
// min/sec registers that get combined into one timestamp, not stored
// as standalone fields.
func isDateTimeComponentField(name string) bool {
	return strings.HasPrefix(name, "START_DATE_TIME_") ||
		strings.HasPrefix(name, "END_DATE_TIME_") ||
		strings.HasPrefix(name, "FILLING_DATE_")
}

// dateTimeComponentSuffixes is the fixed set of parts every
// <prefix>_DATE_TIME_ group must have to be readable.
var dateTimeComponentSuffixes = []string{"YEAR", "MONTH", "DAY", "HOUR", "MIN", "SEC"}

// readDateTime combines a group of single-register fields — e.g.
// START_DATE_TIME_YEAR/MONTH/DAY/HOUR/MIN/SEC — into one time.Time.
// Returns ok=false if any component is missing or unreadable, so the
// caller can fall back to time.Now().
//
// ASSUMPTION: registers hold local factory time, not UTC — swap
// time.Local for time.UTC below if the PLC clock is already UTC.
func readDateTime(jsonPayloads *utils.SafeJsonPayloads, transformations map[string]string, prefix string) (time.Time, bool) {
	parts := make([]int, len(dateTimeComponentSuffixes))
	for i, suffix := range dateTimeComponentSuffixes {
		oldKey, ok := transformations[prefix+suffix]
		if !ok {
			return time.Time{}, false
		}
		raw, exists := jsonPayloads.Get(oldKey)
		if !exists {
			return time.Time{}, false
		}
		val, ok := toInt(raw)
		if !ok {
			return time.Time{}, false
		}
		parts[i] = val
	}

	year, month, day := parts[0], parts[1], parts[2]
	if year < 2000 || month < 1 || month > 12 || day < 1 || day > 31 {
		// Registers exist and parsed as ints, but hold placeholder/
		// not-yet-written zeros rather than a real timestamp — treat
		// as unreadable so the caller falls back to time.Now().
		return time.Time{}, false
	}

	return time.Date(year, time.Month(month), day, parts[3], parts[4], parts[5], 0, time.Local), true
}

// dateOnlyComponentSuffixes is the fixed set of parts every plain
// year/month/day group must have to be readable.
var dateOnlyComponentSuffixes = []string{"YEAR", "MONTH", "DAY"}

// readDateOnly combines a group of single-register year/month/day
// fields — e.g. FILLING_DATE_YEAR/MONTH/DAY — into one time.Time at
// midnight. Mirrors readDateTime but for fields that carry a date
// only, no time-of-day component, so there's no need for the
// STR multi-register decoder — these are plain ints, one per register,
// exactly like START/END_DATE_TIME_* already are.
func readDateOnly(jsonPayloads *utils.SafeJsonPayloads, transformations map[string]string, prefix string) (time.Time, bool) {
	parts := make([]int, len(dateOnlyComponentSuffixes))
	for i, suffix := range dateOnlyComponentSuffixes {
		oldKey, ok := transformations[prefix+suffix]
		if !ok {
			return time.Time{}, false
		}
		raw, exists := jsonPayloads.Get(oldKey)
		if !exists {
			return time.Time{}, false
		}
		val, ok := toInt(raw)
		if !ok {
			return time.Time{}, false
		}
		parts[i] = val
	}

	year, month, day := parts[0], parts[1], parts[2]
	if year < 2000 || month < 1 || month > 12 || day < 1 || day > 31 {
		// placeholder/not-yet-written zeros, same guard as readDateTime
		return time.Time{}, false
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), true
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case string:
		i, err := strconv.Atoi(n)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func storeJobFieldToSession(sess *session.Session, key string, val any) {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	sess.ProcessedPayloadsMap[key] = map[string]any{key: val}
}
