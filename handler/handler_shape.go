package handler

import "strings"

// --------------------------------------------------------------------------
// Envelope shaping for the ps-dashboard `readings` table.
//
// The table shape (see ps-dashboard) is roughly:
//
//	{ tenant_id, device_id, machine_id, lot_id, entity_id, resolution, kind,
//	  created_at, metric_a, metric_b, metric_c,
//	  readings: {...}, output: {...}, status: {...}, limits: {...}, energy: {...} }
//
// gopub-edge doesn't know tenant_id/device_id/lot_id — those get resolved
// downstream (gokafka-raw / your reply engine, keyed off the MQTT topic or
// device registry). All gopub-edge does is bucket the flat key/value data
// it already has into readings / output / limits.
// --------------------------------------------------------------------------

// limitsKeys: SPC bounds -> limits JSONB.
var limitsKeys = map[string]bool{
	"lower_limit": true,
	"standard":    true,
	"upper_limit": true,
}

// outputKeys: actuator states, derived pass/fail classification, lot/model
// identity, and the good/reject/total tally -> output JSONB.
var outputKeys = map[string]bool{
	"do":           true,
	"ch1_remark":   true,
	"ch2_remark":   true,
	"ch3_remark":   true,
	"ink_lot":      true,
	"model_name":   true,
	"good_count":   true,
	"reject_count": true,
	"total_count":  true,
}

// Everything not in limitsKeys/outputKeys falls into readings by default
// (raw process values: ch1_fica1, ch1_tica1, ch{1,2,3}_weighing, vacuum, etc).

// shapeReadingsEnvelope buckets a flat merged payload into the
// readings/output/limits shape. Empty buckets are omitted rather than sent
// as {} so a plain "patch" case (3/4/7/8) with no limits/remarks doesn't
// grow an empty limits/output key.
func shapeReadingsEnvelope(data map[string]any) map[string]any {
	readings := map[string]any{}
	output := map[string]any{}
	limitsOut := map[string]any{}

	for k, v := range data {
		switch {
		case limitsKeys[k]:
			limitsOut[k] = v
		case outputKeys[k]:
			output[k] = v
		default:
			readings[k] = v
		}
	}

	envelope := map[string]any{}
	if len(readings) > 0 {
		envelope["readings"] = readings
	}
	if len(output) > 0 {
		envelope["output"] = output
	}
	if len(limitsOut) > 0 {
		envelope["limits"] = limitsOut
	}
	return envelope
}

// countRemarks tallies good/reject/total across the three channel remarks.
// A channel only counts if its remark key is present in data at all — a
// channel that never ran this cycle shouldn't be counted as either good or
// reject.
func countRemarks(data map[string]any) (good, reject, total int) {
	for _, key := range []string{"ch1_remark", "ch2_remark", "ch3_remark"} {
		v, ok := data[key]
		if !ok || v == nil {
			continue
		}
		total++
		if isNormalRemark(v) {
			good++
		} else {
			reject++
		}
	}
	return good, reject, total
}

// isNormalRemark decides whether a single remark value counts as "good".
//
// utils.RemarkMapping (device/handler side) always writes a string from a
// fixed statusMap — "NORMAL" for the good case, or one of 16 fault labels
// ("OVERLOAD", "PUNCHING MISS/ NO BALL", "BUBBLE", "NO INK", etc.) for a
// reject. Any non-string, or any string other than "NORMAL", counts as reject.
func isNormalRemark(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(s), "NORMAL")
}

// buildReadingsEnvelope is the single entry point every publish call site
// should use: it tallies good/reject/total from any remarks present, folds
// that into the flat data, then shapes the result into readings/output/limits.
func buildReadingsEnvelope(data map[string]any) map[string]any {
	good, reject, total := countRemarks(data)
	if total > 0 {
		data["good_count"] = good
		data["reject_count"] = reject
		data["total_count"] = total
	}
	return shapeReadingsEnvelope(data)
}
