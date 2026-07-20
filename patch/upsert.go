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

// VacuumData is the shape the reply engine (vacuum-engine) sends back on
// the correlated reply topic. XStatus/YStatus are the descriptive IQR
// classification strings ("Within IQR", "Outlier", "Building Data",
// "No Data", ...) — W10/W20 are word/text registers the PLC reads as a
// status label, not boolean coils, so these need to stay strings.
// VacuumStatus is the one real boolean here, written to the M4350 bit
// coil. Mixing string/string/bool in the dataList []any below is fine —
// []any doesn't require uniform element types.
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

// SendUpsertRequest publishes an upsert request over the LOCAL Mosquitto
// broker (vacuum-engine runs on the same edge unit, so there's no reason
// to round-trip through EMQX just to come back down to the same LAN) and
// blocks until vacuum-engine publishes a ReplyPayload back on the
// correlated reply topic (or until replyTimeout elapses). On success it
// parses the reply data exactly like the old PostgREST
// "return=representation" body and writes X/Y/vacuum status back to the
// PLC, unchanged from the HTTP version. vacuum-engine is responsible for
// separately publishing the finished, computed row to EMQX so the general
// insert engine records it.
//
// data should be the fully-shaped envelope (tenant_id, device_id, readings,
// output, limits, status, metric_a/b/c, energy) — mqttpub adds
// reply_topic as a sibling key before publishing, no wrapping field.
func SendUpsertRequest(data map[string]any, cfg config.AppConfig, plcApp *app.Application, replyTimeout time.Duration) ([]byte, error) {
	if Pub == nil {
		return nil, fmt.Errorf("[patch] Pub is not initialized (call mqttpub.NewPublisher and set patch.Pub at startup)")
	}

	reply, err := Pub.PublishAndAwaitReplyLocal(context.Background(), cfg.LocalVacuumRequestTopic, data, replyTimeout)
	if err != nil {
		return nil, fmt.Errorf("[upsert] upsert request failed: %w", err)
	}
	if !reply.Success {
		return nil, fmt.Errorf("[upsert] insert engine reported failure: %s", reply.Error)
	}

	var result []VacuumData
	// Reply engine may send back a single row or an array — same tolerance the old HTTP path had.
	if err := json.Unmarshal(reply.Data, &result); err != nil {
		var single VacuumData
		if err2 := json.Unmarshal(reply.Data, &single); err2 != nil {
			return nil, fmt.Errorf("[upsert] failed to parse reply data: %v", err)
		}
		result = append(result, single)
	}

	if len(result) > 0 && plcApp != nil {
		devicesStr := strings.Split(cfg.Plc.PlcDeviceUpsert, ",")
		if len(devicesStr)%4 != 0 {
			return nil, fmt.Errorf("[upsert] invalid device config string")
		}
		dataList := []any{result[0].YStatus, result[0].XStatus, result[0].VacuumStatus}
		deviceCount := len(devicesStr) / 4
		if deviceCount != len(dataList) {
			return nil, fmt.Errorf("[upsert] mismatch between device count and data count")
		}
		for i := 0; i < deviceCount; i++ {
			deviceStr := strings.Join(devicesStr[i*4:i*4+4], ",")
			if err := plcApp.WritePLC(context.Background(), deviceStr, dataList[i]); err != nil {
				fmt.Printf("[plc] PLC write failed for device %s: %v\n", deviceStr, err)
				return nil, err
			}
		}
	}

	return reply.Data, nil
}
