package handler

import (
	"gopatch/config"
	"gopatch/internal/app"
	"gopatch/internal/session"
	"gopatch/internal/utils"
	"os"
)

// CASE 10, Vacuum; Collect Vacuum Check data to patch.
func handleVacuumCase(session *session.Session, jsonPayloads *utils.SafeJsonPayloads,
	cfg config.AppConfig, rMsgJSONChan <-chan string, plcApp *app.Application) {

	// Check trigger
	triggerValue, ok := jsonPayloads.GetBool(os.Getenv("CASE_10_TRIGGER_UPLOAD"))
	if ok && triggerValue {
		session.Mutex.Lock()
		defer session.Mutex.Unlock()

		MAP_NAME := "healthcheck"
		if session.ProcessedPayloadsMap[MAP_NAME] == nil {
			session.ProcessedPayloadsMap[MAP_NAME] = make(map[string]any)
		}

		for _, channel := range []string{"1min", "2min", "3min"} {
			if val, found := jsonPayloads.Get(os.Getenv("CASE_10_VACUUM_LEAVE_" + channel)); found {
				session.ProcessedPayloadsMap[MAP_NAME]["vacuum_leave_"+channel] = val
			}
		}

		if val2, found := jsonPayloads.Get(os.Getenv("CASE_10_VACUUM_START")); found {
			session.ProcessedPayloadsMap[MAP_NAME]["vacuum_start"] = val2
		}

		session.IsProcessing = true
	}

	if session.IsProcessing {
		keys := []string{
			"healthcheck",
		}
		processPatch(session, keys, cfg, func() {}, rMsgJSONChan, plcApp)
	}

}
