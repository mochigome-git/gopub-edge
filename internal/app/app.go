package app

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"gopub-edge/config"

	MCP "github.com/mochigome-git/msp-go/pkg/mcp"
	PLC "github.com/mochigome-git/msp-go/pkg/plc"
	PLC_Utils "github.com/mochigome-git/msp-go/pkg/utils"
)

// Application is the main application for interacting with the PLC
type Application struct {
	cfg    config.PlcConfig
	logger *log.Logger
	client MCP.Client
	fx     bool
}

// NewApplication initializes the PLC client and creates a new Application instance
func NewApplication(cfg config.PlcConfig, logger *log.Logger) (*Application, error) {
	// Init PLC connection
	if err := PLC.InitMSPClient(cfg.PlcHost, cfg.PlcPort); err != nil {
		return nil, fmt.Errorf("init PLC failed: %w", err)
	}
	logger.Printf("Start communicating with PLC at %s", cfg.PlcHost)

	return &Application{
		cfg:    cfg,
		logger: logger,
		//fx:     cfg.Fx, // Use Fx from config if needed
	}, nil
}

// Close cleanly disconnects from the PLC
func (a *Application) Close() error {
	if a.client == nil {
		return nil
	}
	if err := a.client.Close(); err != nil {
		return fmt.Errorf("failed to close PLC connection: %w", err)
	}
	a.logger.Println("PLC connection closed")
	return nil
}

// writeDataWithContext writes a byte slice to a PLC device with context cancellation
func (a *Application) writeDataWithContext(ctx context.Context, device PLC_Utils.Device, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Call your WriteData method directly
		return PLC.BatchWrite(device.DeviceType, device.DeviceNumber, data, device.NumberRegisters, a.logger)
	}
}

// method to perform a write to plc
func (a *Application) WritePLC(ctx context.Context, deviceStr string, value any) error {
	parts := strings.Split(deviceStr, ",")
	if len(parts) != 4 {
		return fmt.Errorf("invalid device string format, expected 'Type,Number,ProcessNumber,Registers'")
	}

	deviceType := parts[0]
	deviceNumber := parts[1]

	processNumber, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("invalid processNumber: %w", err)
	}

	numberRegisters, err := strconv.Atoi(parts[3])
	if err != nil {
		return fmt.Errorf("invalid numberRegisters: %w", err)
	}

	writeOne := func(valStr string) error {
		data, err := PLC.EncodeData(valStr, processNumber)
		if err != nil {
			return err
		}

		device := PLC_Utils.Device{
			DeviceType:      deviceType,
			DeviceNumber:    deviceNumber,
			NumberRegisters: uint16(numberRegisters),
		}

		a.logger.Printf("Writing to %s%s: % X", deviceType, deviceNumber, data)

		if err := a.writeDataWithContext(ctx, device, data); err != nil {
			return fmt.Errorf("failed to write PLC data: %w", err)
		}
		return nil
	}

	switch v := value.(type) {
	case string:
		return writeOne(v)
	case []string:
		for _, val := range v {
			if err := writeOne(val); err != nil {
				return err
			}
		}
		return nil
	case bool:
		return writeOne(fmt.Sprintf("%t", v))
	case int, int8, int16, int32, int64:
		return writeOne(fmt.Sprintf("%d", v))
	case uint, uint8, uint16, uint32, uint64:
		return writeOne(fmt.Sprintf("%d", v))
	case float32, float64:
		return writeOne(fmt.Sprintf("%f", v))
	default:
		return fmt.Errorf("unsupported value type: %T", value)
	}
}
