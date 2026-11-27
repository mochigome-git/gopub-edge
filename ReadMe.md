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

**Lot from a counter reset (no short pulse to catch).** If the lot button
also resets a counter, watch the counter instead — it stays reset, so a slow
reader still sees it:

```env
CASE_LOT_RESET_REGISTER=d676     # winding meter, reset by the button
CASE_LOT_RESET_MIN_DROP=100      # drop between two readings that counts as a reset
CASE_LOT_COOLDOWN_SEC=30         # default 30 with RESET_REGISTER, else 0
```

The button edge and the counter drop both work; whichever comes first creates
the lot and the cooldown stops the other from creating a second one.
`CASE_LOT_REQUIRE_*` applies to both. The counter must be on the service's
`MQTT_TOPIC`.

**Troubleshooting:** `LOT_DEBUG=true` prints one line per batch, e.g.

```
[lot-debug] batch 312 msgs | x17 n=45 seq=[0×20 1×5 0×20] prev false->false edge=true | d676 n=45 first=8127 last=0 min=0 max=8127 reset=true | conds: d392 > 200 met=true last=0; d418 > 500 met=true last=210
```

`x17 NOT in batch (got: …)` means the trigger isn't arriving on `MQTT_TOPIC`
(wrong topic, register name or case).

### Surviving restarts and long outages

**State file.** Job progress (start time, collected fields, operator), lot
conditions / meter baseline and momentary state buttons are mirrored to
`$STATE_DIR/session-<case>.json` and restored at start.

```env
STATE_DIR=state            # default ./state
STATE_MAX_AGE_HOURS=24     # older files are ignored and deleted
STATE_SAVE_SEC=10          # routine changes at most this often (SD card wear)
# STATE_PERSIST=false      # turn off
```

- Written only when something changed; job start/end, lot latches and
  buttons at once, rising counters at most every `STATE_SAVE_SEC`.
- Written atomically (temp file + rename) — a power cut can't corrupt it.
- **Deleted** when nothing is left to keep (no job running, no latch, …);
  stale or unreadable files are deleted at start.
- The machine state itself is not restored on purpose: the first transition
  after a restart has no `prev_*`, so unobserved time isn't misattributed.

**Disk outbox.** While EMQX is unreachable, posts are written to
`$STATE_DIR/outbox/` (one file each) instead of paho's memory queue, and sent
oldest-first after the reconnect; each file is deleted once the broker
confirms it. New posts queue behind waiting ones, so order is kept.

```env
OUTBOX_MAX_MB=500          # cap; the oldest READINGS are dropped first,
                           # job / lot / state posts are kept
# OUTBOX=false             # turn off
```

Heartbeats are never queued. Rough size: timeseries every 3 s ≈ 150 MB per
day offline; sending a day's backlog takes ~30–60 min after the reconnect.

**Docker:** give each container its own volume, or the state is lost with
the container:

```yaml
    volumes:
      - ./state/mcs-lot:/app/state
```

### Timeseries — keep short button presses

The timeseries case publishes the latest complete scan every
`TIMESERIES_INTERVAL_SEC` (default 3 s), so a button pressed for < 1 s
usually falls between snapshots and is never stored. List such bits in
`TIMESERIES_HOLD_ON`: if one was ON in any scan since the last publish, the
next row stores it as 1.

```env
TIMESERIES_HOLD_ON=x17          # winding meter reset button
```

### Timeseries — full register set every row (carry-forward)

Registers arrive as separate MQTT messages, so a batch often closes
mid-cycle: one holds ~128 of 141 registers and the rest land in the next.
Every batch is merged into a last-value cache and each row is the cache —
every register's newest value, if younger than `TIMESERIES_MAX_AGE_SEC`
(default 3 × interval). `TIMESERIES_MIN_REGISTERS` now checks that merged
row, so set it to the full count (or a little under): it only rejects when
registers really stop arriving. `TIMESERIES_HOLD_ON` bits are never carried
forward as ON, so one press is still stored once.

```env
TIMESERIES_MIN_REGISTERS=135      # of 141; trips when the PLC / MQTT feed drops
TIMESERIES_MAX_AGE_SEC=9          # optional, default 3 × TIMESERIES_INTERVAL_SEC
TIMESERIES_CARRY_FORWARD=false    # optional, back to per-batch rows
```

### Cycle mode — one full PLC scan per row

The legacy burst flusher cuts the MQTT stream on a 2 s timer and on 150 ms
gaps, so one PLC scan can be split over several rows and charts show missing
fields. Cycle mode groups messages by scan instead:

```env
CYCLE_MODE_ENABLED=true
CYCLE_ANCHOR_TOPIC=x26     # a device msp-edge publishes once per scan
CYCLE_TIMEOUT_SEC=6
```

- The anchor can be the full topic (`holding_register/all/X26`) or just the
  device name (`x26`, case-insensitive).
- Pick a device in its own block (the msp-edge startup log lists the blocks;
  `X16 x1 words, 1 devices` is ideal), so it marks the boundary between scans.
- While cycles are detected the legacy flusher is bypassed. If the anchor is
  never seen, or goes silent for `CYCLE_TIMEOUT_SEC`, the legacy flusher takes
  over again, so a wrong anchor never stops data — it logs
  `anchor "…" not seen in 2000 messages`.

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
