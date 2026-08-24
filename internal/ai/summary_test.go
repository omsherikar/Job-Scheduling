package ai

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

func TestLedger(t *testing.T) {
	at := time.Date(2026, 8, 24, 9, 0, 0, 0, time.UTC)
	got := ledger("payments", []jobs.Failure{
		{Class: "timeout", Count: 90, Variants: 2, Sample: "context deadline exceeded", First: at, Last: at},
		{Class: "http_502", Count: 4, Variants: 1, Sample: "bad gateway", First: at, Last: at},
	})

	for _, want := range []string{"payments", "timeout", "90 failures across 2", "context deadline exceeded", "http_502"} {
		if !strings.Contains(got, want) {
			t.Errorf("ledger missing %q:\n%s", want, got)
		}
	}
	if i, j := strings.Index(got, "timeout"), strings.Index(got, "http_502"); i > j {
		t.Errorf("timeout at %d, http_502 at %d, want timeout first", i, j)
	}
}

func TestClipKeepsRunes(t *testing.T) {
	if got := clip("héllo wörld", 5); got != "héllo..." {
		t.Errorf("clip = %q, want %q", got, "héllo...")
	}
	if got := clip("  short  ", 40); got != "short" {
		t.Errorf("clip = %q, want %q", got, "short")
	}
}

func TestSummarizeWithoutKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	if Ready() {
		t.Fatal("Ready with no key")
	}
	_, err := Summarize(context.Background(), "payments", []jobs.Failure{{Class: "timeout", Count: 1}})
	if !errors.Is(err, errNoKey) {
		t.Errorf("err = %v, want ErrNoKey", err)
	}
}
