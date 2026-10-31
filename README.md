# health_checker

Runs a set of named health checks concurrently and reports an aggregate pass or fail status.

```go
package main

import (
    "context"
    "errors"
    "fmt"

    "health_checker"
)

func main() {
    hc, err := health_checker.New([]health_checker.Checker{
        {
            Name: "postgres",
            Fn: func(ctx context.Context) error {
                return nil // real ping here
            },
        },
        {
            Name: "redis",
            Fn: func(ctx context.Context) error {
                return errors.New("connection refused")
            },
        },
    })
    if err != nil {
        panic(err)
    }

    hc.WithTimeout(2 * time.Second)

    status, results := hc.Run(context.Background())
    fmt.Println(status) // fail
    for _, r := range results {
        fmt.Printf("%-10s %v %s\n", r.Name, r.Err, r.Elapsed)
    }
}
```

## Why this exists

A readiness or liveness probe usually needs to ask several subsystems
whether they are alive. Doing those checks one at a time adds their
latencies together, so a slow dependency can stall the probe even when
everything is healthy. This library fans them out concurrently and
reduces the answer to a single pass/fail verdict plus per-check detail
for logging.

The trade-off: there is no retry, no caching, and no dependency ordering.
Each check is an independent yes/no. If you need retries or circuit
breakers, put them inside the check function you supply.

## Edge cases

- An empty checker list returns `StatusPass` with an empty results slice.
  "No checks configured" is treated as a passing aggregate.
- A check that returns an error (including `context.DeadlineExceeded`
  when the timeout fires) marks the whole run as `StatusFail`, but the
  other checks still run to completion and their results are returned.
- Results are always in registration order, not completion order, so you
  can index back to a name without sorting.
- `New` rejects empty names and nil functions up front so a bad config
  fails at construction rather than at check time.
