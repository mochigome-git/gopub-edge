// job_case.go
package handler

import (
	"strings"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
)

func handleJobCase(
	session *session.Session,
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
) {
	// m-coils can come back as float64 or string "1"/"0" — same
	// ambiguity processChannelTrigger already handles for other cases.
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
	default:
		return
	}

	session.Mutex.Lock()
	wasOn := session.JobInProgress
	session.Mutex.Unlock()

	if buttonOn && !wasOn {
		session.Mutex.Lock()
		session.JobInProgress = true
		session.JobStartedAt = time.Now().UTC()
		session.ProcessedPayloadsMap = make(map[string]map[string]any)
		session.Mutex.Unlock()
	}

	if buttonOn {
		fields := readJobFieldsGeneric(jsonPayloads, "CASE_JOB_")
		for k, v := range fields {
			storeJobFieldToSession(session, k, v)
		}

		operator, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_STR_OPERATOR_RE", 7, 15)
		productCode, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_STR_PRODUCT_CODE_RE", 4, 10)
		productName, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_STR_PRODUCT_NAME_RE", 5, 12)
		fillingCode, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_STR_FILLING_CODE_RE", 4, 10)
		fillingDate, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_STR_FILLING_Date_RE", 4, 10)
		inkName, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_STR_INK_NAME_RE", 7, 15)
		modelName, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_MN_MODEL_NAME_RE", 5, 0)
		inkLot, _ := utils.ReadMultiRegisterString(jsonPayloads, "CASE_JOB_LI_INK_LOT_RE", 3, 0)

		if operator != "" {
			storeJobFieldToSession(session, "operator", operator)
		}
		if productCode != "" {
			storeJobFieldToSession(session, "product_code", productCode)
		}
		if productName != "" {
			storeJobFieldToSession(session, "product_name", productName)
		}
		if fillingCode != "" {
			storeJobFieldToSession(session, "filling_code", fillingCode)
		}
		if fillingDate != "" {
			storeJobFieldToSession(session, "filling_date", fillingDate)
		}
		if inkName != "" {
			storeJobFieldToSession(session, "ink_name", inkName)
		}
		if modelName != "" {
			storeJobFieldToSession(session, "model_name", modelName)
		}
		if inkLot != "" {
			storeJobFieldToSession(session, "ink_lot", inkLot)
		}
		return
	}

	if !buttonOn && wasOn {
		session.Mutex.Lock()
		startedAt := session.JobStartedAt
		session.JobInProgress = false
		session.Mutex.Unlock()

		storeJobFieldToSession(session, "started_at", startedAt.Format(time.RFC3339))
		storeJobFieldToSession(session, "ended_at", time.Now().UTC().Format(time.RFC3339))

		session.Mutex.Lock()
		if fillingCode, ok := session.ProcessedPayloadsMap["filling_code"]; ok && fillingCode["filling_code"] != nil && fillingCode["filling_code"] != "" {
			session.ProcessedPayloadsMap["job_ref"] = map[string]any{"job_ref": fillingCode["filling_code"]}
		} else if inklot, ok := session.ProcessedPayloadsMap["ink_lot"]; ok {
			session.ProcessedPayloadsMap["job_ref"] = map[string]any{"job_ref": inklot["ink_lot"]}
		}
		session.Mutex.Unlock()

		keys := []string{
			"operator", "room_temp", "ink_temp",
			"product_code", "product_name", "filling_code", "filling_date", "ink_name", "ink_lot", "model_name",
			"total_output", "good_output", "reject_output",
			"machine_reject_output",
			"job_ref", "started_at", "ended_at",
		}
		keys = append(keys, rejectDetailKeys()...)

		processPatch(session, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil, true)

		session.Mutex.Lock()
		session.ProcessedPayloadsMap = make(map[string]map[string]any)
		session.Mutex.Unlock()
	}
}

func rejectDetailKeys() []string {
	transformations := utils.GetKeyTransformationsFromEnv("CASE_JOB_")
	var keys []string
	for fieldName := range transformations {
		if strings.HasPrefix(fieldName, "reject_") && fieldName != "reject_output" {
			keys = append(keys, fieldName)
		}
	}
	return keys
}

func readJobFieldsGeneric(jsonPayloads *utils.SafeJsonPayloads, envPrefix string) map[string]any {
	transformations := utils.GetKeyTransformationsFromEnv(envPrefix)
	result := make(map[string]any)
	for newKey, oldKey := range transformations {
		if value, exists := jsonPayloads.Get(oldKey); exists {
			result[newKey] = value
		}
	}
	return result
}

func storeJobFieldToSession(sess *session.Session, key string, val any) {
	sess.Mutex.Lock()
	defer sess.Mutex.Unlock()
	sess.ProcessedPayloadsMap[key] = map[string]any{key: val}
}
