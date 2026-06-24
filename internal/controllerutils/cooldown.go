// Package controllerutils holds controller helpers shared across services
// that all build database-backed informer-driven controllers and want
// consistent cadence/gating behavior.
package controllerutils

import (
	"context"
	"time"

	utilsclock "k8s.io/utils/clock"
	"k8s.io/utils/lru"
)

// CooldownChecker decides whether a key may be (re-)queued.
//
// Implementations must be safe to call concurrently — informer event
// handlers, periodic resyncs, and worker goroutines may all consult the
// same checker.
type CooldownChecker interface {
	CanSync(ctx context.Context, key any) bool
}

// TimeBasedCooldownChecker is a fixed-interval cooldown gate: after
// CanSync returns true for a key, subsequent calls for the same key
// return false until cooldownDuration has elapsed since the allowed call.
type TimeBasedCooldownChecker struct {
	clock            utilsclock.PassiveClock
	cooldownDuration time.Duration
	nextExecTime     *lru.Cache
}

// NewTimeBasedCooldownChecker constructs a checker bound to the real
// wall-clock and a 1M-entry LRU.
func NewTimeBasedCooldownChecker(cooldownDuration time.Duration) *TimeBasedCooldownChecker {
	return &TimeBasedCooldownChecker{
		clock:            utilsclock.RealClock{},
		cooldownDuration: cooldownDuration,
		nextExecTime:     lru.New(1000000),
	}
}

// SetClock substitutes the time source used to evaluate the cooldown.
func (c *TimeBasedCooldownChecker) SetClock(clock utilsclock.PassiveClock) {
	c.clock = clock
}

// CanSync stamps now+cooldownDuration on a true return so subsequent calls
// within the cooldown window return false.
func (c *TimeBasedCooldownChecker) CanSync(_ context.Context, key any) bool {
	now := c.clock.Now()

	nextExecTime, ok := c.nextExecTime.Get(key)
	if !ok || now.After(nextExecTime.(time.Time)) {
		c.nextExecTime.Add(key, now.Add(c.cooldownDuration))
		return true
	}
	return false
}
