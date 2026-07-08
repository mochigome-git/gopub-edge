package handler

import (
	"gopatch/config"
	"gopatch/internal/app"
	"gopatch/internal/session"
	"gopatch/internal/utils"
	"gopatch/model"
	"os"
)

type AccumCheckFunc func() bool // Check Accumalate Rate if 0 skip process

func Trigger(
	session *session.Session,
	jsonPayloads *utils.SafeJsonPayloads,
	messages []model.Message,
	cfg config.AppConfig,
	rMsgJSONChan <-chan string,
	plcApp *app.Application,
) {

	// Parse trigger keys once
	triggerKeys := utils.ParseTriggerKey(cfg.Trigger)

	// Avoiding repeated logic for accum_rate checks
	isAccRate := func() bool {
		accum_rate, exists := jsonPayloads.GetFloat64(os.Getenv("CASE_4_AVOID_0"))
		return exists && accum_rate == 0
	}

	// Iterate over trigger keys
	for _, tk := range triggerKeys {
		// Map of case keys to handler functions
		caseHandlers := map[string]func(){
			"time.duration":     func() { handleTimeDurationCase(tk, jsonPayloads, messages, cfg.Loop) },
			"standard":          func() { handleStandardCase(tk, jsonPayloads, messages, cfg) },
			"trigger":           func() { handleTriggerCase(tk, jsonPayloads, messages, cfg) },
			"hold":              func() { handleHoldCase(session, jsonPayloads, messages, cfg, isAccRate) },
			"special":           func() { handleSpecialCase(session, tk, jsonPayloads, messages, cfg) },
			"holdfilling":       func() { handleHoldFillingCase(session, jsonPayloads, messages, cfg, rMsgJSONChan) },
			"weight":            func() { handleWeight(session, jsonPayloads, messages, cfg, false, isAccRate, rMsgJSONChan) },
			"holdfillingweight": func() { handleHoldFillingWeightCase(session, jsonPayloads, messages, cfg, rMsgJSONChan) },
			"holdmcs":           func() { handleHoldMCSCase(session, jsonPayloads, messages, cfg, rMsgJSONChan) },
			"vacuum":            func() { handleVacuumCase(session, jsonPayloads, cfg, rMsgJSONChan, plcApp) },
			"weightmcs":         func() { handleWeightMCSCase(session, jsonPayloads, messages, cfg, false, isAccRate, rMsgJSONChan) },
		}
		// Check if the current caseKey is in the map, and handle accordingly
		if handler, exists := caseHandlers[tk.CaseKey]; exists {
			handler()
		}
	}
}
