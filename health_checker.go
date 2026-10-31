package health_checker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Checker is a named health check function that the Checker runs concurrently.
// It receives a context and returns nil if the subsystem is healthy, or an
// error describing the failure.
type Checker struct {
	Name string
	Fn   func(context.Context) error
}

// Result holds the outcome of a single named health check.
type Result struct {
	Name    string
	Err     error
	Elapsed time.Duration
}

// Status is the aggregate verdict across all registered checks.
// It is "pass" only when every individual Result has a nil Err.
type Status string

const (
	StatusPass Status = "pass"
	StatusFail Status = "fail"
)

// HealthChecker owns a fixed set of named checks and runs them concurrently
// on each call to Run. It is safe for concurrent use after construction.
// Checks are never reused; create a new instance if you need to swap them.
type HealthChecker struct {
	checks []Checker
	timeout time.Duration
}

// New builds a HealthChecker from the supplied checkers.
// If any check has an empty Name or a nil Fn, New returns an error
// immediately rather than failing later inside Run — bad config should
// surface at construction time, not at check time.
func New(checks []Checker) (*HealthChecker, error) {
	for i, c := range checks {
		if c.Name == "" {
			return nil, fmt.Errorf("check at index %d has an empty name", i)
		}
		if c.Fn == nil {
			return nil, fmt.Errorf("check %q has a nil function", c.Name)
		}
	}
	return &HealthChecker{checks: checks}, nil
}

// WithTimeout sets a per-run deadline applied to every check function.
// A value of zero (the default) means no timeout is applied beyond whatever
// the caller's context already carries. Returns the receiver for chaining.
// Must be called before Run.
func (h *HealthChecker) WithTimeout(d time.Duration) *HealthChecker {
	h.timeout = d
	return h
}

// Run executes all registered checks concurrently with a fan-out/fan-in
// pattern. Each check runs in its own goroutine; results are collected in
// the order they were registered so callers can map an index back to a
// name without sorting.
//
// The returned Status is StatusFail if any check returns an error (including
// context.DeadlineExceeded when a timeout fires); otherwise StatusPass.
// An empty checker is considered passing — "no checks required" is a
// legitimate aggregate verdict.
func (h *HealthChecker) Run(ctx context.Context) (Status, []Result) {
	results := make([]Result, len(h.checks))
	if len(h.checks) == 0 {
		return StatusPass, results
	}

	var wg sync.WaitGroup
	for i, c := range h.checks {
		wg.Add(1)
		go func(i int, c Checker) {
			defer wg.Done()
			results[i] = h.runOne(ctx, c)
		}(i, c)
	}
	wg.Wait()

	for _, r := range results {
		if r.Err != nil {
			return StatusFail, results
		}
	}
	return StatusPass, results
}

// runOne executes a single check, applying the configured timeout by deriving
// a child context. We time the call regardless of outcome so the caller can
// see how long an unhealthy subsystem took to respond, which is often as
// useful as the error itself.
func (h *HealthChecker) runOne(ctx context.Context, c Checker) Result {
	if h.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.timeout)
		defer cancel()
	}
	start := time.Now()
	err := c.Fn(ctx)
	return Result{Name: c.Name, Err: err, Elapsed: time.Since(start)}
}
