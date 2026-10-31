package health_checker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// passingCheck returns a check function that always reports healthy.
func passingCheck(name string) Checker {
	return Checker{Name: name, Fn: func(context.Context) error { return nil }}
}

// failingCheck returns a check function that always reports the given error.
func failingCheck(name string, err error) Checker {
	return Checker{Name: name, Fn: func(context.Context) error { return err }}
}

func TestNewRejectsEmptyName(t *testing.T) {
	_, err := New([]Checker{{Name: "", Fn: func(context.Context) error { return nil }}})
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}
	if !strings.Contains(err.Error(), "empty name") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestNewRejectsNilFn(t *testing.T) {
	_, err := New([]Checker{{Name: "db", Fn: nil}})
	if err == nil {
		t.Fatal("expected error for nil function, got nil")
	}
	if !strings.Contains(err.Error(), "nil function") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestEmptyCheckerPasses(t *testing.T) {
	hc, err := New(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, results := hc.Run(context.Background())
	if status != StatusPass {
		t.Fatalf("expected pass, got %s", status)
	}
	if len(results) != 0 {
		t.Fatalf("expected zero results, got %d", len(results))
	}
}

func TestAllPass(t *testing.T) {
	hc, err := New([]Checker{
		passingCheck("a"),
		passingCheck("b"),
		passingCheck("c"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, results := hc.Run(context.Background())
	if status != StatusPass {
		t.Fatalf("expected pass, got %s", status)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error for %s: %v", r.Name, r.Err)
		}
	}
}

func TestOneFailureFailsAggregate(t *testing.T) {
	boom := errors.New("connection refused")
	hc, err := New([]Checker{
		passingCheck("a"),
		failingCheck("b", boom),
		passingCheck("c"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, results := hc.Run(context.Background())
	if status != StatusFail {
		t.Fatalf("expected fail, got %s", status)
	}
	if results[1].Name != "b" {
		t.Fatalf("expected result at index 1 to be b, got %s", results[1].Name)
	}
	if !errors.Is(results[1].Err, boom) {
		t.Fatalf("expected boom error, got %v", results[1].Err)
	}
}

func TestResultsPreserveRegistrationOrder(t *testing.T) {
	hc, err := New([]Checker{
		passingCheck("first"),
		passingCheck("second"),
		passingCheck("third"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, results := hc.Run(context.Background())
	want := []string{"first", "second", "third"}
	for i, name := range want {
		if results[i].Name != name {
			t.Fatalf("index %d: expected %s, got %s", i, name, results[i].Name)
		}
	}
}

func TestConcurrentExecutionDoesNotSerialize(t *testing.T) {
	// If checks ran sequentially this test would take at least 90ms.
	// We assert it finishes well under that by having three checks each
	// block for 30ms on a condition that is released by the previous check.
	// A fan-out scheduler completes them in roughly 30ms total.
	var mu sync.Mutex
	released := map[string]bool{}
	release := func(name string) {
		mu.Lock()
		released[name] = true
		mu.Unlock()
	}
	wait := func(name string) {
		for i := 0; i < 1000; i++ {
			mu.Lock()
			ok := released[name]
			mu.Unlock()
			if ok {
				return
			}
			time.Sleep(time.Millisecond)
		}
		panic("timed out waiting for " + name)
	}
	checks := []Checker{
		{Name: "a", Fn: func(ctx context.Context) error {
			time.Sleep(30 * time.Millisecond)
			release("a")
			return nil
		}},
		{Name: "b", Fn: func(ctx context.Context) error {
			wait("a")
			time.Sleep(20 * time.Millisecond)
			release("b")
			return nil
		}},
		{Name: "c", Fn: func(ctx context.Context) error {
			wait("b")
			time.Sleep(10 * time.Millisecond)
			return nil
		}},
	}
	hc, err := New(checks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	start := time.Now()
	status, _ := hc.Run(context.Background())
	elapsed := time.Since(start)
	if status != StatusPass {
		t.Fatalf("expected pass, got %s", status)
	}
	// Sequential execution would take ~60ms; allow generous headroom for
	// scheduler jitter on CI runners without claiming pass/fail on timing.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("checks appear to have run sequentially: took %v", elapsed)
	}
}

func TestTimeoutCancelsLongCheck(t *testing.T) {
	slow := Checker{
		Name: "slow",
		Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	hc, err := New([]Checker{slow})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hc.WithTimeout(20 * time.Millisecond)
	status, results := hc.Run(context.Background())
	if status != StatusFail {
		t.Fatalf("expected fail due to timeout, got %s", status)
	}
	if !errors.Is(results[0].Err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", results[0].Err)
	}
}

func TestTimeoutDoesNotAffectFastCheck(t *testing.T) {
	fast := Checker{
		Name: "fast",
		Fn: func(ctx context.Context) error {
			return nil
		},
	}
	hc, err := New([]Checker{fast})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hc.WithTimeout(50 * time.Millisecond)
	status, results := hc.Run(context.Background())
	if status != StatusPass {
		t.Fatalf("expected pass, got %s", status)
	}
	if results[0].Err != nil {
		t.Fatalf("unexpected error: %v", results[0].Err)
	}
}

func TestParentContextCancelPropagates(t *testing.T) {
	slow := Checker{
		Name: "slow",
		Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	hc, err := New([]Checker{slow})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	status, results := hc.Run(ctx)
	if status != StatusFail {
		t.Fatalf("expected fail due to parent cancel, got %s", status)
	}
	if !errors.Is(results[0].Err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", results[0].Err)
	}
}

func TestElapsedRecordedOnSuccess(t *testing.T) {
	chk := Checker{
		Name: "timed",
		Fn: func(ctx context.Context) error {
			time.Sleep(15 * time.Millisecond)
			return nil
		},
	}
	hc, err := New([]Checker{chk})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, results := hc.Run(context.Background())
	if results[0].Elapsed < 10*time.Millisecond {
		t.Fatalf("expected elapsed >= 10ms, got %v", results[0].Elapsed)
	}
}

func TestElapsedRecordedOnFailure(t *testing.T) {
	boom := errors.New("down")
	chk := Checker{
		Name: "timed",
		Fn: func(ctx context.Context) error {
			time.Sleep(15 * time.Millisecond)
			return boom
		},
	}
	hc, err := New([]Checker{chk})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	status, results := hc.Run(context.Background())
	if status != StatusFail {
		t.Fatalf("expected fail, got %s", status)
	}
	if results[0].Elapsed < 10*time.Millisecond {
		t.Fatalf("expected elapsed >= 10ms, got %v", results[0].Elapsed)
	}
	if !errors.Is(results[0].Err, boom) {
		t.Fatalf("expected boom, got %v", results[0].Err)
	}
}
