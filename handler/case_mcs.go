package handler

import (
	"fmt"
	"gopub-edge/config"
	"gopub-edge/internal/session"
	"gopub-edge/internal/utils"
	"gopub-edge/model"
	"os"
	"strconv"
	"strings"
	"sync"
)

// CASE 9, HoldMCS; hold the data and wait for the MCS system trigger to
// collect data to patch. Multi-register string fields (ink_lot,
// model_name, product_code, filling_code, ink_name) are decoded via
// utils.ConvertAndStoreMultiStringRegisterFields and only make it into
// the patch if their registers actually held data this cycle —
// resolvePatchKeys drops any of case9PatchKeys that never got set
// (blank registers) instead of patching them in as "", and lets any
// extra field beyond the fixed list (e.g. a newly added multi-register
// field) ride along automatically without a code change here.
func handleHoldMCSCase(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
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

	utils.ChangeName(jsonPayloads)
	utils.ConvertAndStoreMultiStringRegisterFields(jsonPayloads)
	utils.RemarkMapping(jsonPayloads, session)
	utils.StoreFlattenedPayloadToSession(jsonPayloads, session)

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

		if shouldPatch("case9", prevDo, session) {

			// case9PatchKeys is the baseline shape processPatch/downstream
			// expects for this case — resolvePatchKeys filters it down to
			// whatever's actually present rather than sending every key
			// unconditionally.
			var case9PatchKeys = []string{
				"ch1", "ch2", "ch3", "do", "weightch1_", "weightch2_", "weightch3_",
				"ink_lot", "model_name", "lower_limit", "standard", "upper_limit", "target",
				"ch1_remark", "ch2_remark", "ch3_remark",
			}

			keys := resolvePatchKeys(session, case9PatchKeys)
			processPatch(session, keys, cfg, func() { prevDo = false }, rMsgJSONChan, nil)
		}

	}

}

// CASE10, WeightMCS; hold the data and wait until the weighing scale
// trigger fires to collect data to patch. Same patch-key resolution as
// case9: utils.ConvertAndStoreMultiStringRegisterFields decodes
// ink_lot/model_name/product_code/filling_code/ink_name, and
// resolvePatchKeys only includes a case10PatchKeys entry if it actually
// got set this cycle (skips blank-register fields instead of patching
// them in as ""), while still picking up any field outside the fixed
// list automatically.
func handleWeightMCSCase(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message,
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

	utils.ChangeName(jsonPayloads)
	utils.ConvertAndStoreMultiStringRegisterFields(jsonPayloads)
	utils.RemarkMapping(jsonPayloads, session)
	utils.StoreFlattenedPayloadToSession(jsonPayloads, session)

	// Process CH1, CH2, CH3 Weight Triggers
	// Check if all weight triggers (CH1, CH2, CH3) are inactive, but were previously active

	processWeightTriggers(session, jsonPayloads, messages)
	if shouldPatch("case10", chance, session) {

		var case10PatchKeys = []string{
			"ch1_", "ch2_", "ch3_", "vacuum", "weightch1_", "weightch2_", "weightch3_",
			"counterch_", "ink_lot", "model_name", "lower_limit", "standard", "upper_limit", "target",
			"ch1_remark", "ch2_remark", "ch3_remark",
		}

		keys := resolvePatchKeys(session, case10PatchKeys)
		processPatch(session, keys, cfg, func() { session.IsProcessing = false }, rMsgJSONChan, nil)
	}

}

// resolvePatchKeys filters fixedKeys down to only those present in
// session.ProcessedPayloadsMap, then appends any key present in the map
// that isn't in fixedKeys at all. This is what keeps a blank-register
// field (e.g. ink_lot when its PLC registers haven't been written yet)
// from being force-included in a patch as an empty string, while still
// letting new fields show up without editing the caller's fixed list.
func resolvePatchKeys(session *session.Session, fixedKeys []string) []string {
	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	inFixedList := make(map[string]bool, len(fixedKeys))
	keys := make([]string, 0, len(session.ProcessedPayloadsMap))

	for _, k := range fixedKeys {
		inFixedList[k] = true
		if _, ok := session.ProcessedPayloadsMap[k]; ok {
			keys = append(keys, k)
		}
	}

	for k := range session.ProcessedPayloadsMap {
		if !inFixedList[k] {
			keys = append(keys, k)
		}
	}

	return keys
}

// Procees to assigning the common logic to a function and then call that function inside each case
// Handle the common logic for case string and float64;
func processAndPrint(session *session.Session, key string, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message, prevWeightValue *float64) {
	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	processed := utils.ProcessTriggerGeneric(jsonPayloads, messages, func(payload *utils.SafeJsonPayloads) map[string]any {
		if old, exists := session.ProcessedPayloadsMap[key]; exists {
			session.Prev = utils.DeepCopyMap(old)
		}

		updatedMap := utils.Hold_changeName_generic(payload, "HOLD_KEY_TRANSOFRMATION_"+key, session)

		keysToCheck := []string{"ch3_weighing", "ch1_weighing", "ch2_weighing"}
		compareAndUpdateNestedMap(session.ProcessedPayloadsMap, key, updatedMap, keysToCheck, prevWeightValue)

		return updatedMap
	})

	//fmt.Println(session.ProcessedPayloadsMap)
	if processed != nil {
		session.ProcessedPayloadsMap[key] = processed
	}
}

// Helper function to process the trigger for each channel;
// for CASE 4 & CASE 7 & CASE 9 & CASE 10
func processChannelTrigger(triggerEnvVar, prefix string, jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message, session *session.Session) {

	TRIGGER, ok := jsonPayloads.Get(os.Getenv(triggerEnvVar))
	if !ok {
		// fmt.Printf("Trigger key %s not found", os.Getenv(triggerEnvVar))
		return
	}
	switch v := TRIGGER.(type) {
	case string:
		if v == "1" {
			processAndPrint(session, prefix, jsonPayloads, messages, nil)
		}
	case float64:
		if v == 1 {
			processAndPrint(session, prefix, jsonPayloads, messages, nil)
		}
	}
}

// Helper function for assigning the common logic
// to a function and then call that function inside each case
// Handle the common logic for case if not nil;
// for CASE 4 & CASE 7 $ CASE 9 & CASE 10.
func processAndPrintforVacuum(key string, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message, session *session.Session) {
	session.ProcessedPayloadsMap[key] = utils.ProcessTriggerGeneric(jsonPayloads, messages,
		func(payload *utils.SafeJsonPayloads) map[string]any {
			session.Prev = session.ProcessedPayloadsMap[key]
			return utils.Hold_changeName_generic(payload, "CASE_4_VACUUM_", session)
		})
}

// Process for weight triggers (CH1, CH2, CH3); for CASE 7 & CASE 8 & CASE 9 & CASE 10
func processWeightTriggers(session *session.Session, jsonPayloads *utils.SafeJsonPayloads, messages []model.Message) {
	var wg sync.WaitGroup

	// A helper function to process each weight trigger concurrently
	processWeightTrigger := func(channel string, triggerKey string, weightTrigger *bool,
		prevWeightTrigger *bool, prevWeightValue *float64) {

		defer wg.Done()

		triggerValue, ok := jsonPayloads.GetDC(os.Getenv(triggerKey))
		//	fmt.Println(triggerKey, ":", triggerValue)
		if !ok {
			// fmt.Printf("Trigger key %s not found\n", os.Getenv(triggerKey))
			return
		}

		isTriggered := false
		switch v := triggerValue.(type) {
		case string:
			isTriggered = (v == "1")
		case float64:
			isTriggered = (v == 1)
		default:
			fmt.Printf("Unexpected type for trigger value: %T\n", v)
			return
		}

		if isTriggered {
			processAndPrint(session, channel, jsonPayloads, messages, prevWeightValue)
			*weightTrigger = true
			*prevWeightTrigger = true
		} else {
			*weightTrigger = false
		}
	}

	// Add three goroutines to the WaitGroup
	wg.Add(3)

	// Run each trigger processing in its own goroutine
	go processWeightTrigger("weightch1_", "CASE_7_TRIGGER_WEIGHING_CH1", &session.WeightTriggerCh1, &session.PrevWeightTriggerCh1, session.PrevWeightValueCh1)
	go processWeightTrigger("weightch2_", "CASE_7_TRIGGER_WEIGHING_CH2", &session.WeightTriggerCh2, &session.PrevWeightTriggerCh2, session.PrevWeightValueCh2)
	go processWeightTrigger("weightch3_", "CASE_7_TRIGGER_WEIGHING_CH3", &session.WeightTriggerCh3, &session.PrevWeightTriggerCh3, session.PrevWeightValueCh3)

	// Wait for all goroutines to finish
	wg.Wait()
}
