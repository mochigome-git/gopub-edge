package handler

import (
	"fmt"
	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
	"gopub-edge/patch"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// CASE 3, Trigger; handling the device when triggered and hold for 4second to collect data to patch.
func handleTriggerCase(
	tk utils.TriggerKey,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
) {
	if value, ok := jsonPayloads.GetFloat64(tk.TriggerKey); ok && value == 1 {
		startTime := time.Now()
		processMessagesLoop(jsonPayloads, messages, startTime, cfg.Loop)

		if _filter, ok := jsonPayloads.GetFloat64(cfg.Filter); ok && _filter != 0 {
			utils.CalculateAndStoreInklot(jsonPayloads)
			utils.ChangeName(jsonPayloads)

			payloadData := jsonPayloads.GetData()
			envelope := buildReadingsEnvelope(payloadData, cfg)

			// Publish over MQTT instead of hitting Supabase directly.
			// A publish failure here is transient (broker blip, etc.) —
			// log it instead of panicking the whole process.
			if err := patch.SendPatchRequest(envelope); err != nil {
				log.Println("Error publishing patch request:", err)
				return
			}

			elapsedTime := time.Since(startTime)
			prettyPrintJSONWithTime(envelope, elapsedTime)
		}
	}
}

// CASE 4, Hold; hold the data and wait until patch trigger
func handleHoldCase(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	cfg config.AppConfig, checkAccumulateRate AccumCheckFunc) {

	if checkAccumulateRate() {
		return
	}

	// handle the different types (string and float64) of CH1_TRIGGER.
	// And Store the Filling parameter of CH1 when the trigger is true.
	processChannelTrigger("CASE_4_TRIGGER_CH1", "ch1_", jsonPayloads, messages, session)
	processChannelTrigger("CASE_4_TRIGGER_CH2", "ch2_", jsonPayloads, messages, session)
	processChannelTrigger("CASE_4_TRIGGER_CH3", "ch3_", jsonPayloads, messages, session)

	VACUUM_TRIGGER, _ := jsonPayloads.Get(os.Getenv("CASE_4_VACUUM_reach_20pa"))
	if VACUUM_TRIGGER != nil {
		processAndPrintforVacuum("vacuum", jsonPayloads, messages, session)
	}

	if sealing, ok := jsonPayloads.GetFloat64(os.Getenv("CASE_4_SEALING")); ok {
		if sealing == 1 {
			// Use the function with the condition
			//processAndPrintforVacuum("vacuum", jsonPayloads, messages, loop)
			value, exists := jsonPayloads.Get("vacuum")
			if exists {
				fmt.Println(value)
			} else {
				fmt.Println("Key not found")
			}

			// After the goroutine has finished, set prevSealing = sealing
			session.PrevSealing = sealing
		} else if sealing == 0 && session.PrevSealing == 1 {
			// Use the function to merge payloads
			data := mergeNonEmptyMaps(
				session.ProcessedPayloadsMap["ch1_"],
				session.ProcessedPayloadsMap["ch2_"],
				session.ProcessedPayloadsMap["ch3_"],
				session.ProcessedPayloadsMap["vacuum"],
			)

			startTime := time.Now()
			envelope := buildReadingsEnvelope(data, cfg)

			if err := patch.SendPatchRequest(envelope); err != nil {
				log.Println("Error publishing patch request:", err)
				return
			}

			elapsedTime := time.Since(startTime)
			prettyPrintJSONWithTime(envelope, elapsedTime)
			// Update the previous state of sealing
			session.PrevSealing = sealing
		}
	}
}

// CASE 6, HoldFilling; handling the device when triggered and hold for 4second to collect data to patch.
func handleHoldFillingCase(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	cfg config.AppConfig, rMsgJSONChan <-chan string) {

	triggerChannels := []string{"ch1", "ch2", "ch3"}

	for _, channel := range triggerChannels {
		// Retrieve NUMBERofSTATE from environment variable and convert to float64
		NUMBERofSTATEStr := os.Getenv("CASE_6_TRIGGER_NUMBERofSTATE")
		NUMBERofSTATE, err := strconv.ParseFloat(NUMBERofSTATEStr, 64)
		if err != nil {
			fmt.Println("Error parsing NUMBERofSTATE:", err)
			continue
		}

		// Retrieve trigger value from JSON payload
		triggerValue, ok := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_" + channel))
		if ok && triggerValue == NUMBERofSTATE {
			session.Mutex.Lock()
			defer session.Mutex.Unlock()

			session.ProcessedPayloadsMap[channel][channel+"_fill"] = 1
			session.IsProcessing = true
		}
	}

	// Check if all channels are successful and processing is active
	// Use a flag to track if all channels have success = 0
	ch1, ok1 := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_ch1"))
	ch2, ok2 := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_ch2"))
	ch3, ok3 := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_ch3"))
	session.AllSuccessZero = ok1 && ok2 && ok3 && ch1 == 0 && ch2 == 0 && ch3 == 0

	if session.AllSuccessZero && session.IsProcessing {
		prevDo := false

		session.ProcessedPayloadsMap["do"] = utils.ProcessTriggerGeneric(jsonPayloads, messages, func(payload *utils.SafeJsonPayloads) map[string]any {
			prevDo = true
			return utils.Hold_changeName_generic(payload, "CASE_6_DO_", nil)
		})

		processWeightTriggers(session, jsonPayloads, messages)
		if shouldPatch("case8", prevDo, session) {
			keys := []string{
				"ch1", "ch2", "ch3", "do",
			}
			processPatch(session, keys, cfg, func() { prevDo = false }, rMsgJSONChan, nil)
		}

	}

}

// CASE 7, Weight; hold the data and wait until weighing scale trigger to collect data to patch.
func handleWeight(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	cfg config.AppConfig, chance bool, checkAccumulateRate AccumCheckFunc, rMsgJSONChan <-chan string) {

	if checkAccumulateRate() {
		chance = true
	}

	// Process to handling counter when ch1 started
	processChannelTrigger("CASE_4_TRIGGER_CH1", "counterch_", jsonPayloads, messages, session)

	// Process triggers for each channel
	// Handle different types (string and float64) of CH1_TRIGGER, CH2_TRIGGER, CH3_TRIGGER.
	for _, channel := range []string{"ch1_", "ch2_", "ch3_"} {
		processChannelTrigger("CASE_4_TRIGGER_"+strings.ToUpper(channel[:3]), channel, jsonPayloads, messages, session)
	}

	// Process Vacuum Trigger
	vacuumTrigger, _ := jsonPayloads.Get(os.Getenv("CASE_4_VACUUM_reach_20pa"))
	if vacuumTrigger != nil {
		processAndPrintforVacuum("vacuum", jsonPayloads, messages, session)
	}

	// Process CH1, CH2, CH3 Weight Triggers
	// Check if all weight triggers (CH1, CH2, CH3) are inactive, but were previously active
	processWeightTriggers(session, jsonPayloads, messages)
	if shouldPatch("case7", chance, session) {
		keys := []string{
			"ch1_", "ch2_", "ch3_", "vacuum", "weightch1_", "weightch2_", "weightch3_", "counterch_",
		}
		processPatch(session, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil)
	}

}

// CASE 8, HoldFillingWeight; hold the data and wait until weighing scale trigger to collect data to patch.
func handleHoldFillingWeightCase(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	cfg config.AppConfig, rMsgJSONChan <-chan string) {

	for _, channel := range []string{"ch1", "ch2", "ch3"} {
		// Retrieve NUMBERofSTATE from environment variable and convert to float64
		NUMBERofSTATEStr := os.Getenv("CASE_6_TRIGGER_NUMBERofSTATE")
		NUMBERofSTATE, err := strconv.ParseFloat(NUMBERofSTATEStr, 64)
		if err != nil {
			fmt.Println("Error parsing NUMBERofSTATE:", err)
			continue
		}

		triggerValue, ok := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_" + channel))
		if ok && triggerValue == NUMBERofSTATE {
			session.Mutex.Lock()

			if session.ProcessedPayloadsMap[channel] == nil {
				session.ProcessedPayloadsMap[channel] = make(map[string]any)
			}
			session.ProcessedPayloadsMap[channel][channel+"_fill"] = 1
			session.Mutex.Unlock()
			session.IsProcessing = true
		}
	}

	// Check if all channels are successful and processing is active
	// Use a flag to track if all channels have success = 0
	ch1, ok1 := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_ch1"))
	ch2, ok2 := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_ch2"))
	ch3, ok3 := jsonPayloads.GetFloat64(os.Getenv("CASE_6_TRIGGER_ch3"))
	session.AllSuccessZero = ok1 && ok2 && ok3 && ch1 == 0 && ch2 == 0 && ch3 == 0

	if session.AllSuccessZero && session.IsProcessing {
		prevDo := false
		session.ProcessedPayloadsMap["do"] = utils.ProcessTriggerGeneric(jsonPayloads, messages, func(payload *utils.SafeJsonPayloads) map[string]any {
			prevDo = true
			return utils.Hold_changeName_generic(payload, "CASE_6_DO_", nil)
		})

		processWeightTriggers(session, jsonPayloads, messages)

		if shouldPatch("case8", prevDo, session) {
			keys := []string{
				"ch1", "ch2", "ch3", "do", "weightch1_", "weightch2_", "weightch3_",
			}
			processPatch(session, keys, cfg, func() { prevDo = false }, rMsgJSONChan, nil)
		}

	}

}

// Helper Function to merges non-empty maps and returns a new map.
func mergeNonEmptyMaps(maps ...map[string]any) map[string]any {
	result := make(map[string]any)

	for _, m := range maps {
		if len(m) > 0 {
			for key, value := range m {
				result[key] = value
			}
		}
	}

	return result
}

// Helper function to compares and updates values in a nested map based on the provided keys.
// It updates the map if the new value is larger than the existing one; for CASE 7 only
func compareAndUpdateNestedMap(parentMap map[string]map[string]any, parentKey string,
	updateData map[string]any, keysToCheck []string, prevWeightValue *float64) {

	nestedMap := parentMap[parentKey]
	if nestedMap == nil {
		return
	}

	for _, checkKey := range keysToCheck {
		// Retrieve the existing value from the nested map and check if it's a float64
		// If the existing value is greater than the previous weight value, update it
		existingFloat, okExist := nestedMap[checkKey].(float64)

		// Retrieve the new value from the updateData and validate it (must be a non-zero float64)
		newValue, okNew := updateData[checkKey].(float64)

		if !okNew {
			// Key missing in updateData, so fallback to prevWeightValue
			if okExist {
				continue // keep existing value
			}
			// Only restore if existing doesn't exist (new map or was cleared)
			if prevWeightValue != nil {
				nestedMap[checkKey] = *prevWeightValue
			}
			continue
		}

		if newValue == 0 {
			continue
		}

		// fmt.Println("Comparing:", checkKey, newValue, existingFloat, *prevWeightValue)

		if !okExist {
			nestedMap[checkKey] = newValue
			*prevWeightValue = newValue
			continue
		}

		// If the new value is greater than the existing one and greater than or equal to the previous weight
		if newValue > existingFloat && newValue >= *prevWeightValue {
			// fmt.Println("Updating value:", checkKey, existingFloat, "->", newValue, "prevWeight:", *prevWeightValue)
			nestedMap[checkKey] = newValue
			*prevWeightValue = newValue
		} else if newValue >= *prevWeightValue {
			// update prevWeightValue to avoid being stuck at 0
			*prevWeightValue = newValue
		}
	}
}
