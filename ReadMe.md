# MQTT to REST Data Pipeline for IoT Devices

This project is a Go-based data processing service that connects to an MQTT broker, listens to specified topics, parses incoming JSON messages, applies trigger-based processing logic, and patches or posts the transformed data to a RESTful API.

---

## 📦 Features

- Connects to secure MQTT brokers with TLS support.
- Reads MQTT payloads and unmarshals them into structured messages.
- Applies rule-based logic based on trigger devices and operational modes.
- Sends data to a RESTful API endpoint with support for various use cases:
  - Standard posting
  - Trigger-based patching
  - Data holding and batch combining
  - Special processing modes (e.g., weighing, filling, etc.)
- Thread-safe in-memory data storage for handling parallel operations.

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/mochigome-git/gopatch.git
cd gopatch
```

### 2. 🛠️Uncomment LoadEnv (local test)

```bash
sudo nano main.go

//utils.LoadEnv(".env.local")
↓
utils.LoadEnv(".env.local")
```

### 3. 🔧Environment Variables

#### Create a .env.local file with your configuration. Below is a minimal example:

https://github.com/mochigome-git/gopatch/blob/main/.env.example

```bash
# MQTT
MQTT_HOST=mqtt.example.com
MQTT_PORT=8883
MQTT_TOPIC="topic/+"
MQTTS_ON=true

ECS_MQTT_CA_CERTIFICATE="your-ca-cert"
ECS_MQTT_CLIENT_CERTIFICATE="your-client-cert"
ECS_MQTT_PRIVATE_KEY="your-client-key"

# API
API_URL="http://your-api-endpoint"
SERVICE_ROLE_KEY="your-service-role"
BASH_API="POST"

# Trigger
TRIGGER_DEVICE=d800,holdfillingweight
LOOPING=0.5

```

### Lot case — conditions before a new lot

`TRIGGER_DEVICE=x26,lot` creates a new `production.lot` (via the lot
envelope, needs `MACHINE_ID`) on the OFF→ON edge of `x26`. Optional
conditions gate that edge — **any number**, one env var each:

```env
CASE_LOT_REQUIRE_d418=>1500     # meter went above 1500
CASE_LOT_REQUIRE_d392=>200      # speed went above 200
# CASE_LOT_REQUIRE_m50==1       # add more as needed
# CASE_LOT_CONDITION_MODE=latched   # or: now
```

- `<condition>`: optional `>`, `>=`, `<`, `<=`, `=`, `!=` then a number; a bare
  number means `>`. Register names are case-insensitive (`D418` = `d418`).
- `latched` (default): each condition only has to be true at **some** poll
  since the last lot. A meter-reset button is pressed after the machine has
  stopped — the speed is already 0 and the meter may be cleared in that same
  poll — so checking only at the press would never pass.
- `now`: every condition must be true in the poll where the trigger turns ON.
- A press with an unmet condition is ignored and logged
  (`x26 ON ignored — lot continues; unmet (latched): d418 > 1500 (last 820)`);
  the current lot continues and the conditions met so far are kept.
- Every register used must arrive on the service's `MQTT_TOPIC`, together
  with the trigger.
- Conditions met before a gopub-edge restart are not remembered.
- No `CASE_LOT_REQUIRE_*` → every press creates a lot, as before.

### Machine state case — when "running" counts

`TRIGGER_DEVICE=m34,mcsState` tracks running / idle / downtime
(priority: fault alarm > downtime button > running > idle) and publishes one
message per transition. By default the run bit alone decides. Optional rules
make "running" stricter — all must hold, and are ANDed with the run bit:

```env
CASE_MCS_STATE_RUN_START=m34                 # optional once a rule below is set
CASE_MCS_STATE_RUN_REQUIRE_d392=>200         # speed above 200 (same syntax as CASE_LOT_REQUIRE_*)
CASE_MCS_STATE_RUN_COUNT=m28,m29,m30,m31,m32,m33
CASE_MCS_STATE_RUN_COUNT_MIN=4               # at least 4 of them ON (default: all)
CASE_MCS_STATE_RUN_ON_DELAY_SEC=10           # must hold 10 s before -> running
CASE_MCS_STATE_RUN_OFF_DELAY_SEC=30          # must fail 30 s before -> idle

CASE_MCS_STATE_DOWNTIME_START=m36            # HMI downtime bit (level), or START/STOP buttons
```

- Register names are case-insensitive. A register missing from a poll keeps
  its last value.
- The delays stop a short speed dip from logging a stop/start pair. The first
  poll after a restart is taken as-is, so a restart while running doesn't log
  a false idle period.
- When the rule result flips it is logged, e.g.
  `run condition not met — m34 ok, d392 > 200 ok (300), count 3/4 NOT met`.
- No `RUN_REQUIRE_*` / `RUN_COUNT` → behaves exactly as before.

### 4. Run

```
go run main.go
```
