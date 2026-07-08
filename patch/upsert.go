package patch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopub-edge/config"
	"gopub-edge/internal/app"
)

type VacuumData struct {
	ID              string   `json:"id"`
	CreatedAt       string   `json:"created_at"`
	VacuumStart     int      `json:"vacuum_start"`
	VacuumLeave1min float64  `json:"vacuum_leave_1min"`
	VacuumLeave2min float64  `json:"vacuum_leave_2min"`
	VacuumLeave3min float64  `json:"vacuum_leave_3min"`
	XStatus         string   `json:"x_status"`
	YStatus         string   `json:"y_status"`
	VacuumStatus    bool     `json:"vacuum_status"`
	X               *float64 `json:"x"`
	Y               *float64 `json:"y"`
}

// SendUpsertRequest publishes an upsert request to EMQX and blocks until
// your reply engine publishes a ReplyPayload back on the correlated reply
// topic (or until replyTimeout elapses). On success it parses the reply
// data exactly like the old PostgREST "return=representation" body and
// writes X/Y/vacuum status back to the PLC, unchanged from the HTTP version.
func SendUpsertRequest(jsonPayload []byte, cfg config.AppConfig, plcApp *app.Application, replyTimeout time.Duration) ([]byte, error) {
	if Pub == nil {
		return nil, fmt.Errorf("patch: Pub is not initialized (call mqttpub.NewPublisher and set patch.Pub at startup)")
	}

	reply, err := Pub.PublishAndAwaitReply(context.Background(), "upsert", jsonPayload, replyTimeout)
	if err != nil {
		return nil, fmt.Errorf("upsert request failed: %w", err)
	}
	if !reply.Success {
		return nil, fmt.Errorf("insert engine reported failure: %s", reply.Error)
	}

	var result []VacuumData
	// Reply engine may send back a single row or an array — same tolerance the old HTTP path had.
	if err := json.Unmarshal(reply.Data, &result); err != nil {
		var single VacuumData
		if err2 := json.Unmarshal(reply.Data, &single); err2 != nil {
			return nil, fmt.Errorf("failed to parse reply data: %v", err)
		}
		result = append(result, single)
	}

	if len(result) > 0 && plcApp != nil {
		devicesStr := strings.Split(cfg.Plc.PlcDeviceUpsert, ",")
		if len(devicesStr)%4 != 0 {
			return nil, fmt.Errorf("invalid device config string")
		}
		dataList := []any{result[0].YStatus, result[0].XStatus, result[0].VacuumStatus}
		deviceCount := len(devicesStr) / 4
		if deviceCount != len(dataList) {
			return nil, fmt.Errorf("mismatch between device count and data count")
		}
		for i := 0; i < deviceCount; i++ {
			deviceStr := strings.Join(devicesStr[i*4:i*4+4], ",")
			if err := plcApp.WritePLC(context.Background(), deviceStr, dataList[i]); err != nil {
				fmt.Printf("PLC write failed for device %s: %v\n", deviceStr, err)
				return nil, err
			}
		}
	}

	return reply.Data, nil
}
