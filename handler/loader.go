package handler

import (
	"encoding/json"
	"fmt"
	"os"

	"gopub-edge/config"
	"gopub-edge/internal/app"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
	"log"
	"strings"
	"time"
)

// Time process to handling data
var stopProcessing = make(chan struct{})

func ProcessMQTTData(
	cfg config.AppConfig,
	receivedMessagesJSONChan <-chan string,
	plcApp *app.Application,
) {
	// Create a persistent session once
	// Use unique key per logical case
	caseKey := cfg.Function + "_" + cfg.Trigger
	session := session.GetOrCreateSession(caseKey)

	// Create a map to store all JSON payloads
	jsonPayloads := utils.NewSafeJsonPayloads()
	for {
		select {
		case jsonString := <-receivedMessagesJSONChan:
			if jsonString == "" {
				fmt.Println("JSON string is empty")
				continue
			}

			var messages []model.Message

			if err := json.Unmarshal([]byte(jsonString), &messages); err != nil {
				fmt.Printf("Error unmarshaling JSON: %v\n", err)
				// time.Sleep(time.Second)
				continue
			}

			// Prepare JSON payloads for each message
			for _, message := range messages {
				fieldNameLower := strings.ToLower(message.Address)
				fieldValue := message.Value
				jsonPayloads.Set(fieldNameLower, fieldValue)
			}

			// Start to collect data when trigger specify device
			// collect the data for few seconds, process for further handling method.
			// Change Payloads title or delete the extra devices and etc..
			Trigger(session, jsonPayloads, messages, cfg, receivedMessagesJSONChan, plcApp)
			jsonPayloads.Clear()

			return

		case <-stopProcessing:
			return
		}
	}
}

// To stop the goroutine, you can close the stopProcessing channel:
func StopProcessing() {
	close(stopProcessing)
}

func drainChannel(ch <-chan string) {
	for {
		select {
		case <-ch:
			// Discard the value
		default:
			// Exit when there's nothing left
			return
		}
	}
}

// prettyPrintJSONWithTime handles both map[string]interface{} and *SafeJsonPayloads types
func prettyPrintJSONWithTime(data any, duration time.Duration) {
	// Handle nil data case
	if data == nil {
		log.Println("Error: Provided data is nil.")
		return
	}

	// Determine output mode from env
	debugMode := os.Getenv("DEBUG_FULL_JSON") == "true"

	var payload map[string]any

	switch v := data.(type) {
	case map[string]any:
		payload = v
	case *utils.SafeJsonPayloads:
		payload = v.GetData()
	default:
		log.Println("Error: Unsupported data type.")
		return
	}

	// Define ANSI escape codes for colors
	greenColor := "\x1b[32m" // Green color for time
	pinkColor := "\x1b[35m"  // Pink color for JSON
	resetColor := "\x1b[0m"  // Reset color to default

	// Format time
	elapsedTime := fmt.Sprintf("%s%.2f s%s", greenColor, float64(duration.Seconds()), resetColor)

	if debugMode {
		// Full JSON print
		formatted, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			fmt.Println("Error formatting JSON:", err)
			return
		}
		jsonFormatted := fmt.Sprintf("%s%s%s", pinkColor, string(formatted), resetColor)
		log.Printf(">= %s %s", elapsedTime, jsonFormatted)
	} else {
		// Summary only
		count := len(payload)
		log.Printf(">= %s updated %d fields", elapsedTime, count)
	}
}
