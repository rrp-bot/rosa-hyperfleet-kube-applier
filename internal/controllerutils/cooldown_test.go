package controllerutils

import (
	"context"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

func TestTimeBasedCooldownChecker_FirstCallAllowed(t *testing.T) {
	checker := NewTimeBasedCooldownChecker(10 * time.Minute)
	if !checker.CanSync(context.Background(), "key-1") {
		t.Error("first call should be allowed")
	}
}

func TestTimeBasedCooldownChecker_SecondCallBlocked(t *testing.T) {
	checker := NewTimeBasedCooldownChecker(10 * time.Minute)
	checker.CanSync(context.Background(), "key-1")
	if checker.CanSync(context.Background(), "key-1") {
		t.Error("second call within cooldown should be blocked")
	}
}

func TestTimeBasedCooldownChecker_DifferentKeysIndependent(t *testing.T) {
	checker := NewTimeBasedCooldownChecker(10 * time.Minute)
	checker.CanSync(context.Background(), "key-1")
	if !checker.CanSync(context.Background(), "key-2") {
		t.Error("different key should be allowed")
	}
}

func TestTimeBasedCooldownChecker_AllowedAfterCooldown(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fakeClock := clocktesting.NewFakePassiveClock(now)

	checker := NewTimeBasedCooldownChecker(10 * time.Minute)
	checker.SetClock(fakeClock)

	if !checker.CanSync(context.Background(), "key-1") {
		t.Fatal("first call should be allowed")
	}
	if checker.CanSync(context.Background(), "key-1") {
		t.Fatal("second call within cooldown should be blocked")
	}

	fakeClock.SetTime(now.Add(11 * time.Minute))
	if !checker.CanSync(context.Background(), "key-1") {
		t.Error("call after cooldown elapsed should be allowed")
	}
}

func TestTimeBasedCooldownChecker_RepeatedFalseDoesNotPreventTrue(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fakeClock := clocktesting.NewFakePassiveClock(now)

	checker := NewTimeBasedCooldownChecker(10 * time.Minute)
	checker.SetClock(fakeClock)

	checker.CanSync(context.Background(), "key-1")

	// Multiple false calls should not extend the cooldown
	for i := range 5 {
		fakeClock.SetTime(now.Add(time.Duration(i+1) * time.Minute))
		if checker.CanSync(context.Background(), "key-1") {
			t.Fatalf("call at +%dm should be blocked", i+1)
		}
	}

	fakeClock.SetTime(now.Add(11 * time.Minute))
	if !checker.CanSync(context.Background(), "key-1") {
		t.Error("call after cooldown should be allowed despite repeated blocked calls")
	}
}
