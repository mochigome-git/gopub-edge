package handler

import (
	"fmt"
	"gopub-edge/config"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
	"gopub-edge/patch"
	"log"
	"strings"
	"time"
)

var processPrevTriggerKeyMap = make(map[string]string)

// Stopwatch to count the device duration in Case 1.
var deviceStartTimeMap = make(map[string]time.Time)

// CASE 1, time.Duration; handling the process of time taken from 0 to 1, and record the total time duration
func handleTimeDurationCase(tk utils.TriggerKey, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	loop float64) {
	processKey := generateProcessKey(tk.TriggerKey)
	if tk.TriggerKey != processPrevTriggerKeyMap[processKey] {
		processPrevTriggerKeyMap[processKey] = tk.TriggerKey
		if trigger, ok := jsonPayloads.GetFloat64(tk.TriggerKey); ok && trigger != 0 {
			handleTimeDurationTrigger(tk, jsonPayloads, messages, loop)
		}
	}
}

// CASE 2, Standard; handling a devices value and patch it, when the trigger is different with previous key
func handleStandardCase(tk utils.TriggerKey, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message, cfg config.AppConfig) {
	processKey := generateProcessKey(tk.TriggerKey)
	if tk.TriggerKey != processPrevTriggerKeyMap[processKey] {
		processPrevTriggerKeyMap[processKey] = tk.TriggerKey
		if trigger, ok := jsonPayloads.GetFloat64(tk.TriggerKey); ok && trigger != 0 {
			var startTime time.Time
			processMessagesLoop(jsonPayloads, messages, startTime, cfg.Loop)
			utils.CalculateAndStoreInklot(jsonPayloads)
			utils.ChangeName(jsonPayloads)

			if trigger, ok := jsonPayloads.GetFloat64(tk.TriggerKey); ok && trigger == 0 {
				fmt.Println("Case 1")
				fmt.Println(jsonPayloads)

				payloadData := jsonPayloads.GetData()
				envelope := buildReadingsEnvelope(payloadData, cfg)

				if err := patch.SendPatchRequest(envelope); err != nil {
					log.Println("Error publishing patch request:", err)
					return
				}

				elapsedTime := time.Since(startTime)
				prettyPrintJSONWithTime(envelope, elapsedTime)
			}
		}
	}
}

// Process to check the time taken from 0 to 1; or CASE 1
func handleTimeDurationTrigger(tk utils.TriggerKey, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message, loop float64,
) {
	if val, ok := jsonPayloads.Get(tk.TriggerKey); ok {
		fmt.Printf("Device name: %s, Payload: %v\n", tk.TriggerKey, val)
	} else {
		fmt.Printf("Device name: %s, Payload: <no data>\n", tk.TriggerKey)
	}

	if startTime, exists := deviceStartTimeMap[tk.TriggerKey]; !exists {
		deviceStartTimeMap[tk.TriggerKey] = time.Now()
	} else {
		if trigger, ok := jsonPayloads.GetFloat64(tk.TriggerKey); ok && trigger == 0 {
			utils.CalculateAndStoreInklot(jsonPayloads)
			utils.ChangeName(jsonPayloads)
			processMessagesLoop(jsonPayloads, messages, startTime, loop)
		}
		deviceStartTimeMap[tk.TriggerKey] = time.Now()
	}
}

// generateProcessKey creates a unique key for each process based on relevant parameters.
func generateProcessKey(triggerKey string) string {
	// You can concatenate relevant parameters to create a unique key
	return triggerKey /* + other parameters as needed */
}

// processMessagesLoop receives messages within a specified time and updates a JSON payload map.
// If a key is repeated, it overwrites the existing value.
func processMessagesLoop(jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	startTime time.Time, loop float64) {
	for {
		for _, message := range messages {
			fieldNameLower := strings.ToLower(message.Address)
			fieldValue := message.Value
			jsonPayloads.Set(fieldNameLower, fieldValue)
		}
		time.Sleep(time.Second)
		if time.Since(startTime).Seconds() >= loop {
			break
		}
	}
}
