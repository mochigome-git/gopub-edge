// outbox.go — disk queue for posts made while EMQX is unreachable.
//
// paho queues QoS 1 publishes in memory while disconnected, which is lost
// on a restart and runs out of message IDs (~65k) on a long outage. With
// the outbox, a post made while the broker is down is written to its own
// file instead and sent, oldest first, once the connection is back:
//
//	OUTBOX=false            # turn off (back to paho's in-memory queue)
//	OUTBOX_DIR=state/outbox # default: $STATE_DIR/outbox
//	OUTBOX_MAX_MB=500       # disk cap; oldest READINGS are dropped first,
//	                        # job / lot / state posts are kept to the end
//
// File maintenance:
//   - one file per post, written atomically; deleted as soon as the broker
//     confirms it — an empty directory means nothing is waiting
//   - stray temp files from a power cut are removed at start
//   - while anything is waiting, new posts queue behind it, so the order
//     (job end -> lot -> state change) is kept
//   - heartbeats are never queued: a late heartbeat means nothing
package mqttpub

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type outbox struct {
	dir      string
	maxBytes int64

	mu    sync.Mutex
	count int
	bytes int64
	seq   atomic.Uint64

	dropped int // readings dropped for the cap since the last log
}

func outboxFromEnv(stateDir string) *outbox {
	if os.Getenv("OUTBOX") == "false" {
		return nil
	}
	dir := strings.TrimSpace(os.Getenv("OUTBOX_DIR"))
	if dir == "" {
		dir = filepath.Join(stateDir, "outbox")
	}
	maxMB := 500.0
	if v, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv("OUTBOX_MAX_MB")), 64); err == nil && v > 0 {
		maxMB = v
	}
	ob := &outbox{dir: dir, maxBytes: int64(maxMB * 1024 * 1024)}
	if err := ob.open(); err != nil {
		log.Printf("[outbox] ⚠ cannot use %s (%v) — posts during an outage stay in memory only", dir, err)
		return nil
	}
	return ob
}

// open creates the directory, removes stray temp files and counts what
// is already waiting (e.g. from before a restart).
func (o *outbox) open() error {
	if err := os.MkdirAll(o.dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(o.dir, name))
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		if info, err := e.Info(); err == nil {
			o.count++
			o.bytes += info.Size()
		}
	}
	if o.count > 0 {
		log.Printf("[outbox] %d post(s) (%.1f MB) waiting from before — sending once EMQX is connected", o.count, float64(o.bytes)/1e6)
	}
	return nil
}

func (o *outbox) pending() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.count
}

// put stores one payload. kind is job | lot | state | readings.
func (o *outbox) put(payload []byte, kind string) error {
	name := fmt.Sprintf("%020d-%08d-%s.json", time.Now().UnixNano(), o.seq.Add(1), kind)
	path := filepath.Join(o.dir, name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	o.mu.Lock()
	o.count++
	o.bytes += int64(len(payload))
	over := o.bytes > o.maxBytes
	first := o.count == 1
	o.mu.Unlock()
	if first {
		log.Printf("[outbox] EMQX unreachable — queuing posts on disk in %s", o.dir)
	}
	if over {
		o.enforceCap()
	}
	return nil
}

// list returns the waiting files, oldest first.
func (o *outbox) list() []string {
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // zero-padded timestamp + seq = chronological
	return names
}

func (o *outbox) remove(name string) {
	path := filepath.Join(o.dir, name)
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if os.Remove(path) == nil {
		o.mu.Lock()
		o.count--
		o.bytes -= info.Size()
		if o.count <= 0 {
			o.count, o.bytes = 0, 0
		}
		o.mu.Unlock()
	}
}

func kindOf(name string) string {
	base := strings.TrimSuffix(name, ".json")
	if i := strings.LastIndex(base, "-"); i >= 0 {
		return base[i+1:]
	}
	return ""
}

// enforceCap drops the oldest readings until under the cap; only if no
// readings are left are older event posts dropped.
func (o *outbox) enforceCap() {
	names := o.list()
	for _, onlyReadings := range []bool{true, false} {
		for _, n := range names {
			o.mu.Lock()
			under := o.bytes <= o.maxBytes
			o.mu.Unlock()
			if under {
				goto done
			}
			if onlyReadings && kindOf(n) != "readings" {
				continue
			}
			if !onlyReadings {
				log.Printf("[outbox] ⚠ disk cap reached with no readings left — dropping %s", n)
			}
			o.remove(n)
			o.dropped++
		}
	}
done:
	if o.dropped > 0 && o.dropped%500 == 1 {
		log.Printf("[outbox] ⚠ OUTBOX_MAX_MB reached — dropped %d oldest readings so far (job/lot/state posts are kept)", o.dropped)
	}
}

// drain sends waiting posts oldest first while send succeeds. It stops at
// the first failure and is retried on the next tick.
func (o *outbox) drain(send func([]byte) error) (sent int) {
	for _, n := range o.list() {
		path := filepath.Join(o.dir, n)
		b, err := os.ReadFile(path)
		if err != nil {
			o.remove(n)
			continue
		}
		if err := send(b); err != nil {
			return sent
		}
		o.remove(n)
		sent++
	}
	return sent
}
