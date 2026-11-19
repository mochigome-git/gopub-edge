// job_case.go
package handler

import (
	"fmt"
	"log"
	"os"
	"sort"
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
const jobFieldPrefix = "CASE_JOB_"

// multiRegisterMaxLenOverrides holds the small number of fields that need
// an explicit trim because their real digit count is odd relative to
// register capacity. Empty/missing entries default to 0 (no trim).
// e.g. multiRegisterMaxLenOverrides["operator"] = 15
var multiRegisterMaxLenOverrides = map[string]int{}

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
	if buttonOn && session.JobEnding {
		// Trigger came back before the end was confirmed — a glitch,
		// not a real end. Unfreeze and keep collecting into the same job.
		log.Printf("handleJobCase: trigger back ON after %d OFF frame(s) / %v — not a job end, continuing the same job",
			session.JobOffFrames, jobNow().Sub(session.JobOffSince).Round(time.Millisecond))
		session.JobEnding = false
		session.JobOffFrames = 0
	}
	session.Mutex.Unlock()

	if buttonOn && !wasOn {
		session.Mutex.Lock()
		session.JobInProgress = true
		session.JobEnding = false
		session.JobOffFrames = 0
		session.JobStartedAt = jobNow().UTC()
		session.JobEndedAt = time.Time{}
		session.JobStartedFromPLC = false
		session.JobEndedFromPLC = false
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
			storeStickyJobField(session, k, v)
		}

		// If the PLC gives us a real start timestamp, prefer it over
		// the time.Now() capture above.
		transformations := utils.GetKeyTransformationsFromEnv(jobFieldPrefix)
		if t, ok := readDateTime(jsonPayloads, transformations, "START_DATE_TIME_"); ok {
			session.Mutex.Lock()
			session.JobStartedAt = t
			session.JobStartedFromPLC = true
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
			session.JobEndedFromPLC = true
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
		// from env, not hardcoded per field. Shared with case10's
		// ConvertAndStoreModelName via utils.ReadDiscoveredMultiRegisterFields.
		// job_case doesn't delete consumed keys from jsonPayloads (unlike
		// case10), so the usedKeys return is discarded here.
		fields, _ := utils.ReadDiscoveredMultiRegisterFields(
			jsonPayloads, jobFieldPrefix, []string{"STR", "LI"}, multiRegisterMaxLenOverrides,
		)
		for k, v := range fields {
			if strings.HasPrefix(k, "daily_check_") {
				dailyCheck[k] = v
				continue
			}
			storeJobFieldToSession(session, k, v)
		}

		// 4-register signed 64-bit numbers (HMI 数値入力, 符号付きBIN64):
		// product_code, filling_code — CASE_JOB_I64_<KEY>_RE0..RE3.
		int64Fields, _ := utils.ReadDiscoveredInt64Fields(jsonPayloads, jobFieldPrefix)
		for k, v := range int64Fields {
			storeStickyJobField(session, k, v)
		}

		if len(dailyCheck) > 0 {
			mergeStickyDailyCheck(session, dailyCheck)
		}
		return
	}

	if !buttonOn && wasOn {
		session.Mutex.Lock()
		if !session.JobEnding {
			// First OFF frame: freeze the held job data from here on.
			// The PLC may reset operator/temps/counters right after the
			// trigger drops, so no field is read from OFF frames.
			session.JobEnding = true
			session.JobOffSince = jobNow()
			session.JobOffFrames = 0
		}
		session.JobOffFrames++
		frames, offFor := session.JobOffFrames, jobNow().Sub(session.JobOffSince)
		session.Mutex.Unlock()

		// The end timestamp is the one thing still read while frozen:
		// the PLC writes it at completion, it isn't part of what gets
		// reset, and it may land a frame or two after the trigger drops.
		if t, ok := readDateTime(jsonPayloads, utils.GetKeyTransformationsFromEnv(jobFieldPrefix), "END_DATE_TIME_"); ok {
			session.Mutex.Lock()
			session.JobEndedAt = t
			session.JobEndedFromPLC = true
			session.Mutex.Unlock()
		}

		minFrames, minOff := jobEndDebounce()
		if frames < minFrames || offFor < minOff {
			return // not confirmed yet — wait for more OFF frames
		}
		finishJob(session, cfg, rMsgJSONChan)
	}
}

// finishJob posts a confirmed job end from a frozen snapshot of the held
// data. The snapshot is taken out of the session first, so the session
// starts clean for the next job and nothing that happens to it while
// the request is being built and published can change what is sent.
func finishJob(session *session.Session, cfg config.AppConfig, rMsgJSONChan <-chan string) {
	session.Mutex.Lock()
	startedAt := session.JobStartedAt
	startedFromPLC := session.JobStartedFromPLC
	endedAt := session.JobEndedAt // last PLC end time captured, may be zero
	endedFromPLC := session.JobEndedFromPLC
	offSince := session.JobOffSince

	frozen := session.ProcessedPayloadsMap
	session.ProcessedPayloadsMap = make(map[string]map[string]any)
	session.JobInProgress = false
	session.JobEnding = false
	session.JobOffFrames = 0
	session.Mutex.Unlock()

	if endedAt.IsZero() {
		// No PLC end time: use when the trigger first dropped, not when
		// the debounce confirmed it.
		log.Printf("handleJobCase: END_DATE_TIME_* never captured during job — using the time the trigger dropped for ended_at")
		endedAt = offSince.UTC()
		endedFromPLC = false
	}
	frozen["started_at"] = map[string]any{"started_at": formatJobTime(startedAt, startedFromPLC)}
	frozen["ended_at"] = map[string]any{"ended_at": formatJobTime(endedAt, endedFromPLC)}

	// job_ref = <product_code>-<YYYYMMDD>-<HHMMSS> of the job's end,
	// e.g. 123456-20261103-171205. An empty HMI input reads back as
	// "0" from the int64 registers — treat that as empty. When it's
	// empty, job_ref is left out and gokafka-raw generates one.
	delete(frozen, "job_ref")
	if code, ok := jobRefValue(frozen["product_code"]["product_code"]); ok {
		frozen["job_ref"] = map[string]any{
			"job_ref": code + "-" + jobRefTime(endedAt, endedFromPLC),
		}
	}

	warnMissingSetupFields(frozen)

	// No manually maintained keys list. job_summary.go already knows
	// which of these have dedicated columns (total_output, model_name,
	// ...) and deletes them before marshalling the rest into meta — so
	// whatever we didn't explicitly handle above just rides along and
	// lands in meta automatically.
	keys := make([]string, 0, len(frozen))
	for k := range frozen {
		keys = append(keys, k)
	}

	postJob(session, frozen, keys, cfg, rMsgJSONChan)
}

// postJob publishes the frozen job snapshot; swappable in tests.
var postJob = func(session *session.Session, frozen map[string]map[string]any, keys []string, cfg config.AppConfig, rMsgJSONChan <-chan string) {
	processPatchFrom(session, frozen, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil, envelopeJob)
}

// jobNow is time.Now, swappable in tests.
var jobNow = time.Now

// jobEndDebounce returns how long the trigger must stay OFF before a job
// end is confirmed: at least JOB_END_DEBOUNCE_FRAMES consecutive OFF
// frames (default 3) AND at least JOB_END_DEBOUNCE_SEC seconds (default
// 3, fractions allowed). Both must hold. Set both to 1 / 0 to end on the
// first OFF frame like before.
func jobEndDebounce() (int, time.Duration) {
	frames := 3
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("JOB_END_DEBOUNCE_FRAMES"))); err == nil && v >= 1 {
		frames = v
	}
	dur := 3 * time.Second
	if v, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv("JOB_END_DEBOUNCE_SEC")), 64); err == nil && v >= 0 {
		dur = time.Duration(v * float64(time.Second))
	}
	return frames, dur
}

// setupFieldChecks are the operator-entered fields a job should never end
// without, keyed by output name -> the env var that configures them. A
// field is only checked when its env var is set, so machines that don't
// map it don't get warnings.
var setupFieldChecks = map[string]string{
	"operator":  jobFieldPrefix + "STR_OPERATOR_RE0",
	"ink_temp":  jobFieldPrefix + "ink_temp",
	"room_temp": jobFieldPrefix + "room_temp",
}

// warnMissingSetupFields logs, once per job, any configured setup field
// that is still empty/zero at the end — so a PLC/HMI that cleared or
// never had the input shows up in the edge log the same day instead of
// in review later.
func warnMissingSetupFields(frozen map[string]map[string]any) {
	var missing []string
	for key, envName := range setupFieldChecks {
		if os.Getenv(envName) == "" {
			continue
		}
		if isZeroJobValue(frozen[key][key]) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		log.Printf("handleJobCase: ⚠ job ended with empty setup field(s) %v — the PLC registers held no value during this job", missing)
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
			continue // handled by utils.ReadDiscoveredMultiRegisterFields instead
		}
		if value, exists := jsonPayloads.Get(oldKey); exists {
			result[newKey] = value
		}
	}
	return result
}

func isMultiRegisterKey(name string) bool {
	return strings.HasPrefix(name, "STR_") || strings.HasPrefix(name, "LI_") ||
		strings.HasPrefix(name, "I64_")
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

// plcLocalLayout is how PLC wall-clock times are sent: deliberately with
// NO offset. The edge doesn't know which timezone the factory is in — the
// tenant's timezone lives in public.tenants.timezone, and gokafka-raw's
// insert interprets offset-less values in that zone. Times this service
// generates itself (time.Now()) are real instants and keep their "Z".
const plcLocalLayout = "2006-01-02T15:04:05"

// formatJobTime renders a job timestamp for the envelope: PLC wall-clock
// without an offset, or a UTC instant in RFC3339.
// jobRefValue returns v as a job_ref, or ok=false when it's missing,
// blank, or an all-zero register value (an empty HMI input).
func jobRefValue(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	ref := strings.TrimSpace(fmt.Sprint(v))
	if ref == "" || strings.Trim(ref, "0") == "" {
		return "", false
	}
	return ref, true
}

// jobRefTime renders the job end as YYYYMMDD-HHMMSS in factory local
// time. PLC values are already local wall-clock; the time.Now() fallback
// is UTC, so it is shown in the edge unit's local zone.
func jobRefTime(t time.Time, fromPLC bool) string {
	if !fromPLC {
		t = t.Local()
	}
	return t.Format("20060102-150405")
}

func formatJobTime(t time.Time, fromPLC bool) string {
	if fromPLC {
		return t.Format(plcLocalLayout)
	}
	return t.UTC().Format(time.RFC3339)
}

// readDateTime combines a group of single-register fields — e.g.
// START_DATE_TIME_YEAR/MONTH/DAY/HOUR/MIN/SEC — into one time.Time.
// Returns ok=false if any component is missing or unreadable, so the
// caller can fall back to time.Now().
//
// The registers hold local factory wall-clock time. The returned value
// carries a placeholder UTC zone that is never sent — formatJobTime
// strips it, and the database applies the tenant's timezone. Do not
// compare it against time.Now() instants.
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

	return time.Date(year, time.Month(month), day, parts[3], parts[4], parts[5], 0, time.UTC), true
}

// dateOnlyComponentSuffixes is the fixed set of parts every plain
// year/month/day group must have to be readable.
var dateOnlyComponentSuffixes = []string{"YEAR", "MONTH", "DAY"}

// readDateOnly combines a group of single-register year/month/day
// fields — e.g. FILLING_DATE_YEAR/MONTH/DAY — into one time.Time at
// midnight. Mirrors readDateTime but for fields that carry a date
// only, no time-of-day component.
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

	// Calendar date only; UTC midnight is just the carrier, so the date
	// never shifts with the container's TZ setting.
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), true
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

// storeStickyJobField stores a job field read during the ON phase, but
// never lets a zero/blank reading replace a real value already captured
// for this job. Every ON frame used to overwrite the field, so whatever
// the PLC showed in the LAST frame before the trigger dropped won — and
// if the ladder/HMI clears ink_temp, room_temp, the daily-check grid or
// the reject counters at completion (or a single frame reads back
// zeros), a value that was correct for hours was thrown away and 0 was
// logged instead. Non-zero readings still overwrite, so a corrected
// value or a rising counter keeps updating.
func storeStickyJobField(sess *session.Session, key string, val any) {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	if isZeroJobValue(val) {
		if prev, ok := sess.ProcessedPayloadsMap[key][key]; ok && !isZeroJobValue(prev) {
			return
		}
	}
	sess.ProcessedPayloadsMap[key] = map[string]any{key: val}
}

// mergeStickyDailyCheck merges this frame's daily_check cells into the
// job's daily_check object cell by cell, with the same rule as
// storeStickyJobField. Before, the whole object was replaced each frame.
func mergeStickyDailyCheck(sess *session.Session, frame map[string]any) {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	merged := map[string]any{}
	if prev, ok := sess.ProcessedPayloadsMap["daily_check"]["daily_check"].(map[string]any); ok {
		for k, v := range prev {
			merged[k] = v
		}
	}
	for k, v := range frame {
		if prev, ok := merged[k]; ok && isZeroJobValue(v) && !isZeroJobValue(prev) {
			continue
		}
		merged[k] = v
	}
	sess.ProcessedPayloadsMap["daily_check"] = map[string]any{"daily_check": merged}
}

// isZeroJobValue reports whether a register reading carries no data:
// nil, numeric zero, or a blank/"0" string.
func isZeroJobValue(v any) bool {
	switch n := v.(type) {
	case nil:
		return true
	case float64:
		return n == 0
	case int:
		return n == 0
	case int64:
		return n == 0
	case string:
		t := strings.TrimSpace(n)
		return t == "" || t == "0"
	default:
		return false
	}
}
