package handler

import (
	"fmt"
	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
	"gopub-edge/patch"
	"log"
	"time"
)

// CASE 5, Special; handling a device's highest value and average value and patch it, when the trigger is 1
func handleSpecialCase(session *session.Session, tk utils.TriggerKey, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	cfg config.AppConfig) {
	// Assuming these variables need to be declared and initialized
	var startTime time.Time
	if trigger, ok := jsonPayloads.GetFloat64(tk.TriggerKey); ok {
		if trigger == 1 {
			session.IsProcessing = true
			// Assuming processedPayloadsMap is a map[string]map[string]interface{}
			if session.ProcessedPayloadsMap["degas"]["pica1"] == nil {
				session.ProcessedPayloadsMap["degas"]["pica1"] = make([]float64, 0)
			}
			result := ProcessTriggerGenericSpecial(jsonPayloads, messages, trigger, func(payload *utils.SafeJsonPayloads) map[string]interface{} {
				return utils.Hold_changeName_generic(payload, "CASE_5_DEGAS_", nil)
			})
			// Assuming pica1 is a float64 value in the result map
			if pica1, ok := result["pica1"].(float64); ok {
				session.ProcessedPayloadsMap["degas"]["pica1"] = append(session.ProcessedPayloadsMap["degas"]["pica1"].([]float64), pica1)
			}
			//fmt.Println(session.ProcessedPayloadsMap["degas"]["pica1"])
		}
		if trigger == 0 && session.IsProcessing {
			session.IsProcessing = false
			pica1Values, ok := session.ProcessedPayloadsMap["degas"]["pica1"].([]float64)
			if ok && len(pica1Values) > 0 { // Check if there are values in the slice
				// Calculate max
				max := pica1Values[0]
				for _, value := range pica1Values {
					if value > max {
						max = value
					}
				}
				session.ProcessedPayloadsMap["degas"]["pica1_max"] = max

				// Calculate average
				var sum float64
				for _, value := range pica1Values {
					sum += value
				}
				average := sum / float64(len(pica1Values))
				session.ProcessedPayloadsMap["degas"]["pica1_average"] = average
			} else {
				// Handle the case where there are no values in the pica1Values slice
				fmt.Println("No values found for pica1.")
			}

			// Clear degas values
			delete(session.ProcessedPayloadsMap["degas"], "pica1")

			// Shape into the readings/output/limits/status envelope and publish.
			envelope := buildReadingsEnvelope(session.ProcessedPayloadsMap["degas"], cfg)

			if err := patch.SendPatchRequest(envelope); err != nil {
				log.Println("Error publishing patch request:", err)
				return
			}

			elapsedTime := time.Since(startTime)
			prettyPrintJSONWithTime(envelope, elapsedTime)
			session.ProcessedPayloadsMap["degas"] = make(map[string]interface{})
		}
	}
}

// processMessagesLoop receives messages within a specified time,
// handle average value and highest value of message and updates a JSON payload map.
// If a key is repeated, it overwrites the existing value.

// ProcessTriggerGenericSpecial is a generic function to process trigger key
// and return the corresponding processed payload
// Using for Case Special
func ProcessTriggerGenericSpecial(jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
	loop float64, changeNameFunc func(*utils.SafeJsonPayloads) map[string]interface{}) map[string]interface{} {
	startTime := time.Now()
	processMessagesLoop(jsonPayloads, messages, startTime, 1)
	processedPayload := changeNameFunc(jsonPayloads)
	return processedPayload
}
