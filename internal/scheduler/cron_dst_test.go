package scheduler

import (
	"testing"
	"time"
)

func TestSpringForward(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	after := time.Date(2024, 3, 9, 12, 0, 0, 0, loc)
	tick, err := nextTick("0 12 * * *", "America/New_York", after)
	if err != nil {
		t.Fatal(err)
	}
	local := tick.In(loc)
	if local.Year() != 2024 || local.Month() != time.March || local.Day() != 10 || local.Hour() != 12 {
		t.Fatalf("tick = %s, want 2024-03-10 12:00 local", local)
	}
	if d := tick.Sub(after); d != 23*time.Hour {
		t.Fatalf("delta = %s, want %s", d, 23*time.Hour)
	}
}

func TestFallBack(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	after := time.Date(2024, 11, 2, 12, 0, 0, 0, loc)
	tick, err := nextTick("0 12 * * *", "America/New_York", after)
	if err != nil {
		t.Fatal(err)
	}
	local := tick.In(loc)
	if local.Year() != 2024 || local.Month() != time.November || local.Day() != 3 || local.Hour() != 12 {
		t.Fatalf("tick = %s, want 2024-11-03 12:00 local", local)
	}
	if d := tick.Sub(after); d != 25*time.Hour {
		t.Fatalf("delta = %s, want %s", d, 25*time.Hour)
	}
}
