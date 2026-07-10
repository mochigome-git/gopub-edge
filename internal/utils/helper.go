package utils

import (
	"encoding/json"
	"fmt"
	"gopub-edge/internal/session"
	"gopub-edge/model"
	"log"
	"os"
	"strconv"
	"strings"
)

// Helper function to compares and updates values in a nested map based on the provided keys.
// It updates the map if the new value is larger than the existing one; for CASE 7 only
func CompareAndUpdateNestedMap(parentMap map[string]map[string]interface{}, parentKey string,
	updateData map[string]interface{}, keysToCheck []string, prevWeightValue *float64) {

	nestedMap := parentMap[parentKey]
	if nestedMap == nil {
		return
	}

	for _, checkKey := range keysToCheck {
		// Retrieve the existing value from the nested map and check if it's a float64
		// If the existing value is greater than the previous weight value, update it
		existingFloat, okExist := nestedMap[checkKey].(float64)
		if okExist && existingFloat > *prevWeightValue {
			*prevWeightValue = existingFloat
		}

		// Retrieve the new value from the updateData and validate it (must be a non-zero float64)
		newValue, okNew := updateData[checkKey].(float64)
		if !okNew || newValue == 0 {
			continue
		}

		fmt.Println("Comparing:", checkKey, newValue, existingFloat, *prevWeightValue)

		if !okExist {
			continue
		}

		// If the new value is greater than the existing one and greater than or equal to the previous weight
		if newValue > existingFloat && newValue >= *prevWeightValue {
			fmt.Println("Updating value:", checkKey, existingFloat, "->", newValue, "prevWeight:", *prevWeightValue)
			nestedMap[checkKey] = newValue
			*prevWeightValue = newValue
		}
	}
}

// Helper Function takes an input string and returns a new string with its characters reversed.
func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// Helper Function, a generic function to replace device names in the JSON payload
// with readable keys for a specific case.
func _hold_changeName_generic(jsonPayloads *SafeJsonPayloads, key string) map[string]any {
	// Define a mapping of key transformations
	holdkeyTransformations := GetKeyTransformationsFromEnv(key)
	result := make(map[string]any)

	// Iterate through key transformations and apply them, deleting old keys during transformation
	for newKey, oldKey := range holdkeyTransformations {

		// Replace old key with new key if the old key exists, delete old key
		if value, oldKeyExists := jsonPayloads.Get(oldKey); oldKeyExists {
			//if numericValue, isNumeric := value.(float64); isNumeric && numericValue != 0 {
			result[newKey] = value
			// delete(jsonPayloads, oldKey) - consider whether to delete old keys
			//}
		}
	}

	// Apply the specific transformation function
	return result
}

// Helper Function retrieves key transformations from environment variables based on a given prefix.
func GetKeyTransformationsFromEnv(prefix string) map[string]string {
	keyTransformations := make(map[string]string)

	// Iterate over all environment variables
	for _, env := range os.Environ() {
		// Split the environment variable into key and value
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]

			// Check if the key starts with the specified prefix
			if strings.HasPrefix(key, prefix) {
				// Trim the prefix and add the key-value pair to the map
				key = strings.TrimPrefix(key, prefix)
				keyTransformations[key] = value
			}
		}
	}

	return keyTransformations
}

type TriggerKey struct {
	TriggerKey string
	CaseKey    string
}

// Helper Function splits a string of trigger keys and case numbers,
// returning a slice of TriggerKey structs.
func ParseTriggerKey(triggerKey string) []TriggerKey {
	triggerKeySlice := strings.Split(triggerKey, ",")
	var triggerkeys []TriggerKey

	// Check if the number of items in the triggerKeySlice is even
	if len(triggerKeySlice)%2 != 0 {
		fmt.Println("Warning: Malformed triggerKey input. Ensure it contains pairs of trigger and case numbers.")
		return triggerkeys // Return empty slice if the input is malformed
	}

	for i := 0; i < len(triggerKeySlice); i += 2 {
		caseNumber := triggerKeySlice[i+1]

		triggerkeys = append(triggerkeys, TriggerKey{
			TriggerKey: triggerKeySlice[i],
			CaseKey:    fmt.Sprint(caseNumber),
		})
	}

	return triggerkeys
}

// Helper Function replaces device names in the JSON payload with readable keys.
func ChangeName(jsonPayloads *SafeJsonPayloads) {
	// Define a mapping of key transformations
	keyTransformations := GetKeyTransformationsFromEnv("KEY_TRANSFORMATION_")

	// Repeat channel 1's sequence count (PLC's device name) for channel 2 and channel 3.
	if d760, exists := jsonPayloads.Get("d760"); exists {
		jsonPayloads.Set("ch1_sequence", d760)
		jsonPayloads.Set("ch2_sequence", d760)
		jsonPayloads.Set("ch3_sequence", d760)
	}
	// Remove Channel 1, 2, 3 keys after processing
	jsonPayloads.Delete("d160")
	jsonPayloads.Delete("d460")
	jsonPayloads.Delete("d760")

	// Iterate through key transformations and apply them, deleting old keys during transformation
	for newKey, oldKey := range keyTransformations {
		// Replace old key with new key if the old key exists, delete old key
		if value, exists := jsonPayloads.Get(oldKey); exists {
			jsonPayloads.Set(newKey, value)
			jsonPayloads.Delete(oldKey)
		}
	}
}

// Helper Function to computes and stores an 'ink_lot' value based on specific keys in the JSON payload.
func CalculateAndStoreInklot(jsonPayloads *SafeJsonPayloads) {
	d171Value, d171Exists := jsonPayloads.GetString("d171")
	d172Value, d172Exists := jsonPayloads.GetString("d172")
	d173Value, d173Exists := jsonPayloads.GetString("d173")

	var inklotValue string
	if d171Exists && d172Exists && d173Exists {
		// Concatenate reversed strings of d171, d172, and d173
		inklotValue = reverseString(d171Value) + reverseString(d172Value) + reverseString(d173Value)
	}
	jsonPayloads.Set("ink_lot", inklotValue)

	// Remove "d171", "d172", and "d173" keys from the map
	jsonPayloads.Delete("d171")
	jsonPayloads.Delete("d172")
	jsonPayloads.Delete("d173")
}

// Helper Function, a generic function to replace device names in the JSON payload
// with readable keys for a specific case.
func Hold_changeName_generic(jsonPayloads *SafeJsonPayloads, key string, session *session.Session) map[string]any {
	holdkeyTransformations := GetKeyTransformationsFromEnv(key)
	result := make(map[string]any)

	for newKey, oldKey := range holdkeyTransformations {
		value, exists := jsonPayloads.Get(oldKey)
		numericValue, isNumeric := value.(float64)

		if exists && isNumeric {
			if numericValue != 0 {
				result[newKey] = numericValue
				continue
			}
		}

		if session != nil && session.Prev != nil {
			if prevVal, ok := session.Prev[newKey]; ok {
				result[newKey] = prevVal
			}
		}
	}

	return result
}

func DeepCopyMap(original map[string]any) map[string]any {
	copy := make(map[string]any)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

// ProcessTriggerGeneric is a generic function to process trigger key
// and return the corresponding processed payload
func ProcessTriggerGeneric(jsonPayloads *SafeJsonPayloads, messages []model.Message,
	changeNameFunc func(*SafeJsonPayloads) map[string]any) map[string]any {

	//startTime := time.Now()
	//processMessagesLoop(jsonPayloads, messages, startTime, 1)

	processMessagesOnce(jsonPayloads, messages)
	CalculateAndStoreInklot(jsonPayloads)
	processedPayload := changeNameFunc(jsonPayloads)

	return processedPayload
}

// processMessagesOnce updates the JSON payload map with the given messages.
// If a key is repeated, it overwrites the existing value.
func processMessagesOnce(jsonPayloads *SafeJsonPayloads, messages []model.Message) {
	for _, message := range messages {
		fieldNameLower := strings.ToLower(message.Address)
		fieldValue := message.Value
		jsonPayloads.Set(fieldNameLower, fieldValue)
	}
}

// Helper Function to convert and stores 'model_name' value based on the JSON payload
func ConvertAndStoreModelName(jsonPayloads *SafeJsonPayloads) {
	type task struct {
		envPrefix string
		keyPrefix string
		count     int
		outputKey string
	}

	tasks := []task{
		{"CASE_9_MN_", "MODEL_NAME_RE", 5, "model_name"},
		{"CASE_9_LI_", "INK_LOT_RE", 3, "ink_lot"},
	}

	// Map to collect which keys are used
	usedKeys := make(map[string]bool)

	// Process all tasks first
	for _, t := range tasks {
		keyTransformations := GetKeyTransformationsFromEnv(t.envPrefix)
		var builder strings.Builder

		for i := 0; i <= t.count; i++ {
			envKey := fmt.Sprintf("%s%d", t.keyPrefix, i)
			deviceKey, ok := keyTransformations[envKey]
			if !ok {
				// fmt.Printf("Warning: Missing env key %s\n", envKey)
				continue
			}
			val, ok := jsonPayloads.GetString(deviceKey)
			if !ok {
				// fmt.Printf("Warning: Missing payload key %s (from %s)\n", deviceKey, envKey)
				continue
			}
			reversed := reverseString(val)
			// fmt.Printf("Processing %s → %s → %s → reversed: %s\n", envKey, deviceKey, val, reversed) // Debug
			builder.WriteString(reversed)
			usedKeys[deviceKey] = true
		}

		result := builder.String()
		if result != "" {
			cleaned := sanitizeString(result)
			jsonPayloads.Set(t.outputKey, cleaned)
		}

	}

	// Now safely delete used keys
	for key := range usedKeys {
		jsonPayloads.Delete(key)
	}
}

func StoreFlattenedPayloadToSession(jsonPayloads *SafeJsonPayloads, session *session.Session) {
	jsonPayloads.Range(func(key string, value any) {
		lowerKey := strings.ToLower(key)

		switch v := value.(type) {
		case map[string]any:
			for innerKey, innerVal := range v {
				storeToSession(session, innerKey, innerVal)
			}
		default:
			storeToSession(session, lowerKey, v)
		}
	})
}

func storeToSession(session *session.Session, key string, val any) {
	session.Mutex.Lock()
	defer session.Mutex.Unlock()
	session.ProcessedPayloadsMap[key] = map[string]any{key: val}
}

func sanitizeString(s string) string {
	// Remove null bytes
	s = strings.ReplaceAll(s, "\x00", "")
	// Trim leading/trailing spaces (optional, depending on your PLC data)
	s = strings.TrimSpace(s)
	return s
}

func RemarkMapping(jsonPayloads *SafeJsonPayloads, session *session.Session) {
	statusMap := map[int]string{
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
	}

	// log.Printf("[Remark] ---- jsonPayloads keys ----")
	// jsonPayloads.Range(func(k string, v any) {
	// 	log.Printf("[Remark] key=%q value=%v type=%T", k, v, v)
	// })

	toInt := func(value any) (int, bool) {
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

	lookupRemark := func(key string) string {
		value, exists := jsonPayloads.GetAny(key)
		if !exists || value == nil {
			return statusMap[0]
		}
		code, ok := toInt(value)
		if !ok {
			log.Printf("[Remark] %s: unparseable value %v (%T) → NORMAL", key, value, value)
			return statusMap[0]
		}
		label, found := statusMap[code]
		if !found {
			log.Printf("[Remark] %s: code %d out of range → NORMAL", key, code)
			return statusMap[0]
		}
		return label
	}

	remarks := map[string]string{
		"ch1_remark": lookupRemark("d26"),
		"ch2_remark": lookupRemark("d27"),
		"ch3_remark": lookupRemark("d28"),
	}

	// Latch faults: once a channel shows a non-NORMAL remark, don't let a
	// later NORMAL overwrite it before the patch fires. processPatch clears
	// ProcessedPayloadsMap after each patch, which resets the latch.
	const NormalConfirmCycles = 5 // NORMAL must persist this many cycles to clear a latched fault

	session.Mutex.Lock()
	for key, val := range remarks {
		existing, ok := session.ProcessedPayloadsMap[key]
		prev := ""
		if ok {
			if p, hasPrev := existing[key].(string); hasPrev {
				prev = p
			}
		}

		if prev != "" && prev != "NORMAL" && val == "NORMAL" {
			// incoming NORMAL against a latched fault — count it
			session.RemarkNormalStreak[key]++
			if session.RemarkNormalStreak[key] < NormalConfirmCycles {
				continue // not confirmed yet, keep the fault latched
			}
			// confirmed sustained NORMAL → operator cleared it, allow overwrite
		} else {
			// any non-NORMAL (or first write) resets the streak
			session.RemarkNormalStreak[key] = 0
		}

		session.ProcessedPayloadsMap[key] = map[string]any{key: val}
	}
	session.Mutex.Unlock()

	jsonPayloads.Delete("d26")
	jsonPayloads.Delete("d27")
	jsonPayloads.Delete("d28")
}
