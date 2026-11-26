package session

import (
	"sync"
	"time"
)

var (
	sessionStore = make(map[string]*Session)
	sessionMutex sync.Mutex // prevent race conditions if accessed concurrently
)

type Session struct {
	Mutex                sync.Mutex     // Sync to protect session.ProcessedPayloadsMap
	PrevSealing          float64        // To store the trigger of condition judgement in case 4
	IsProcessing         bool           // Flag to track if the process is active
	FillArmed            bool           // CASE 9 holdmcs fill-cycle latch; separate from IsProcessing, which shouldPatch reads as "patch in flight"
	HeldDo               map[string]any // CASE 9 holdmcs: last d2870 seen ("do"), sent on upload
	DoFrozen             bool           // CASE 9 holdmcs: true from fill end until upload fires; HeldDo stops updating
	AllSuccessZero       bool
	WeightTriggerCh1     bool
	WeightTriggerCh2     bool
	WeightTriggerCh3     bool
	PrevWeightTriggerCh1 bool
	PrevWeightTriggerCh2 bool
	PrevWeightTriggerCh3 bool
	PrevWeightValueCh1   *float64
	PrevWeightValueCh2   *float64
	PrevWeightValueCh3   *float64
	ProcessedPayloadsMap map[string]map[string]any
	RemarkNormalStreak   map[string]int
	Prev                 map[string]any

	// ── job case (充填記録書) ──────────────────────────────────────
	JobInProgress bool      // true between button ON and OFF
	JobStartedAt  time.Time // captured at the ON edge, read back at OFF
	JobEndedAt    time.Time
	// true when the value came from PLC registers (local factory wall-clock,
	// no zone) rather than time.Now() (a real UTC instant). The database
	// applies the tenant's timezone to PLC values — see formatJobTime.
	JobStartedFromPLC bool
	JobEndedFromPLC   bool
	// End debounce: the trigger reading OFF once only starts the end of a
	// job. While JobEnding is true the held job data is frozen (no frame
	// updates it), and the job is posted only after the trigger has stayed
	// OFF long enough; if it comes back ON the same job continues.
	JobEnding    bool
	JobOffSince  time.Time // when the trigger first read OFF
	JobOffFrames int       // consecutive OFF frames seen since JobOffSince
	// Text fields (operator, ...) read completely while idle since the last
	// job ended — seeds the next job, in case the HMI/PLC clears them when
	// START is pressed. ON frames still overwrite.
	JobPreStart map[string]string
	// Fields that came only from JobPreStart in the current job (for logs).
	JobFromPreStart map[string]bool

	// ── lot case ──────────────────────────────────────
	LotTriggerPrev bool // tracks previous scan's button state for edge detection
	// CASE_LOT_RESET_REGISTER (lot_reset.go): last counter value seen,
	// and when the last lot was created (for CASE_LOT_COOLDOWN_SEC).
	LotResetLast   float64
	LotResetHave   bool
	LotLastCreated time.Time
	// CASE_LOT_REQUIRE_* (lot_conditions.go): condition text -> true once it
	// has held since the last lot was created; last value per register.
	LotCondMet  map[string]bool
	LotCondLast map[string]float64

	// ── state case ──────────────────────────────────────
	MachineState       string    // "running" | "idle" | "downtime"
	StateSince         time.Time // when MachineState began (for prev_duration_sec)
	StateKnown         bool      // run signal read at least once since start
	RunRequested       bool
	DowntimeRequested  bool
	AlarmOn            map[string]bool // alarm code -> bit state
	RunStartWasOn      bool
	RunStopWasOn       bool
	DowntimeStartWasOn bool
	DowntimeStopWasOn  bool

	// CASE_MCS_STATE_RUN_* rules (mcs_run_conditions.go)
	RunRuleLast  map[string]float64 // last value per rule register
	RunInit      bool               // debounce seeded
	RunActive    bool               // debounced run decision
	RunRawLast   bool               // raw run condition last poll
	RunRawSince  time.Time          // when the raw condition last flipped
	RunLogged    bool
	RunLoggedVal bool
}

func NewSession() *Session {
	return &Session{
		// Create a map to store processed payloads (ch1, ch2, ch3; _xx_jsonPayloads) for holdCase
		ProcessedPayloadsMap: map[string]map[string]any{
			"ch1_":       make(map[string]any),
			"ch2_":       make(map[string]any),
			"ch3_":       make(map[string]any),
			"ch1":        make(map[string]any),
			"ch2":        make(map[string]any),
			"ch3":        make(map[string]any),
			"vacuum":     make(map[string]any),
			"degas":      make(map[string]any),
			"do":         make(map[string]any),
			"weightch1_": make(map[string]any),
			"weightch2_": make(map[string]any),
			"weightch3_": make(map[string]any),
			"counter":    make(map[string]any),
		},
		PrevWeightValueCh1: new(float64),
		PrevWeightValueCh2: new(float64),
		PrevWeightValueCh3: new(float64),
		AllSuccessZero:     false,
		RemarkNormalStreak: make(map[string]int),
	}
}

// GetOrCreateSession ensures a session exists for a caseKey
func GetOrCreateSession(caseKey string) *Session {
	sessionMutex.Lock()
	defer sessionMutex.Unlock()

	if s, ok := sessionStore[caseKey]; ok {
		return s
	}

	newSession := NewSession()
	sessionStore[caseKey] = newSession
	return newSession
}

func ClearSession(caseKey string) {
	sessionMutex.Lock()
	defer sessionMutex.Unlock()
	delete(sessionStore, caseKey)
}
