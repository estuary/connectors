package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

var (
	errRetryable = errors.New("retryable")
	errFatal     = errors.New("fatal")
)

func classifyTestErr(err error) Outcome {
	if errors.Is(err, errRetryable) {
		return Retryable
	}
	return Fatal
}

var testPolicy = Policy{
	Classify:       classifyTestErr,
	MaxAttempts:    5,
	InitialBackoff: time.Second,
	MaxBackoff:     4 * time.Second,
}

// run calls p.Do with an operation returning errs in order, then nil once
// they run out. It returns Do's error and the elapsed (fake) time at the start
// of each attempt.
func run(ctx context.Context, p Policy, errs ...error) (error, []time.Duration) {
	var start = time.Now()
	var at []time.Duration
	var err = p.Do(ctx, "test op", func(context.Context) error {
		at = append(at, time.Since(start))
		if len(errs) == 0 {
			return nil
		}
		var err = errs[0]
		errs = errs[1:]
		return err
	})
	return err, at
}

func seconds(s ...float64) []time.Duration {
	var out []time.Duration
	for _, v := range s {
		out = append(out, time.Duration(v*float64(time.Second)))
	}
	return out
}

func TestDoOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		policy  Policy
		errs    []error
		wantErr error
		wantAt  []time.Duration
	}{
		{
			name:   "success needs no retry",
			policy: testPolicy,
			wantAt: seconds(0),
		},
		{
			name:   "retryable errors are retried until success",
			policy: testPolicy,
			errs:   []error{errRetryable, fmt.Errorf("wrapped: %w", errRetryable)},
			wantAt: seconds(0, 1, 3),
		},
		{
			name:    "fatal errors are not retried",
			policy:  testPolicy,
			errs:    []error{errFatal},
			wantErr: errFatal,
			wantAt:  seconds(0),
		},
		{
			name:    "a fatal error after retries stops them",
			policy:  testPolicy,
			errs:    []error{errRetryable, errFatal},
			wantErr: errFatal,
			wantAt:  seconds(0, 1),
		},
		{
			name:    "backoff doubles up to its cap, and attempts are bounded",
			policy:  testPolicy,
			errs:    []error{errRetryable, errRetryable, errRetryable, errRetryable, errRetryable, errRetryable},
			wantErr: errRetryable,
			wantAt:  seconds(0, 1, 3, 7, 11),
		},
		{
			name: "elapsed time bounds retries",
			policy: Policy{
				Classify:       classifyTestErr,
				MaxElapsed:     8 * time.Second,
				InitialBackoff: time.Second,
			},
			errs:    []error{errRetryable, errRetryable, errRetryable, errRetryable, errRetryable},
			wantErr: errRetryable,
			// A fourth retry would start at 7+8 = 15s, past the 8s bound.
			wantAt: seconds(0, 1, 3, 7),
		},
		{
			name: "a retry that would start exactly at the elapsed bound is not made",
			policy: Policy{
				Classify:       classifyTestErr,
				MaxElapsed:     3 * time.Second,
				InitialBackoff: time.Second,
			},
			errs:    []error{errRetryable, errRetryable, errRetryable},
			wantErr: errRetryable,
			wantAt:  seconds(0, 1),
		},
		{
			name:    "zero backoff retries immediately",
			policy:  Policy{Classify: classifyTestErr, MaxAttempts: 3},
			errs:    []error{errRetryable, errRetryable},
			wantAt:  seconds(0, 0, 0),
			wantErr: nil,
		},
		{
			name:    "a server-requested delay longer than the backoff is honoured, even past MaxBackoff",
			policy:  testPolicy,
			errs:    []error{WithDelay(errRetryable, 10*time.Second), fmt.Errorf("outer: %w", WithDelay(errRetryable, 7*time.Second))},
			wantAt:  seconds(0, 10, 17),
			wantErr: nil,
		},
		{
			name:    "a server-requested delay shorter than the backoff is ignored",
			policy:  testPolicy,
			errs:    []error{errRetryable, WithDelay(errRetryable, time.Second)},
			wantAt:  seconds(0, 1, 3),
			wantErr: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var err, at = run(t.Context(), tt.policy, tt.errs...)
				if tt.wantErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tt.wantErr)
				}
				require.Equal(t, tt.wantAt, at)
			})
		})
	}
}

func TestDoErrorMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// An error that is not retried is returned as is.
		var err, _ = run(t.Context(), testPolicy, errFatal)
		require.Same(t, errFatal, err)

		// An error after retries says how many attempts were made.
		err, _ = run(t.Context(), testPolicy, errRetryable, errRetryable, errRetryable, errRetryable, errRetryable)
		require.EqualError(t, err, "after 5 attempts: retryable")
		require.ErrorIs(t, err, errRetryable)
	})
}

func TestDoPermanent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var err, at = run(t.Context(), testPolicy, errRetryable, Permanent(errRetryable))
		require.Len(t, at, 2)
		require.EqualError(t, err, "after 2 attempts: retryable")
		require.ErrorIs(t, err, errRetryable)

		require.NoError(t, Permanent(nil))
	})
}

func TestDoJitter(t *testing.T) {
	var policy = Policy{
		Classify:       classifyTestErr,
		MaxAttempts:    3,
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     5 * time.Second,
		Jitter:         0.5,
	}

	for _, tt := range []struct {
		name   string
		rand   float64
		wantAt []time.Duration
	}{
		// Nominal delays are 2s then 4s.
		{name: "lowest draw", rand: 0, wantAt: seconds(0, 1, 3)},
		{name: "middle draw", rand: 0.5, wantAt: seconds(0, 2, 6)},
		// 6s is capped to MaxBackoff.
		{name: "highest draw", rand: 1, wantAt: seconds(0, 3, 8)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var saved = randFloat
			randFloat = func() float64 { return tt.rand }
			t.Cleanup(func() { randFloat = saved })

			synctest.Test(t, func(t *testing.T) {
				var err, at = run(t.Context(), policy, errRetryable, errRetryable)
				require.NoError(t, err)
				require.Equal(t, tt.wantAt, at)
			})
		})
	}
}

func TestDoContext(t *testing.T) {
	t.Run("cancellation during a backoff returns promptly", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var ctx, cancel = context.WithCancel(t.Context())
			var calls int
			var done = make(chan error)
			go func() {
				done <- testPolicy.Do(ctx, "test op", func(context.Context) error {
					calls++
					return errRetryable
				})
			}()
			synctest.Wait() // Do is now waiting out its first backoff.
			cancel()

			var err = <-done
			require.EqualError(t, err, "context canceled while waiting to retry test op after: retryable")
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, errRetryable)
			require.Equal(t, 1, calls)
		})
	})

	t.Run("an error is not retried once the context is done", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var ctx, cancel = context.WithCancel(t.Context())
			var calls int
			var err = testPolicy.Do(ctx, "test op", func(context.Context) error {
				calls++
				cancel()
				return errRetryable
			})
			require.Same(t, errRetryable, err)
			require.Equal(t, 1, calls)
		})
	})

	t.Run("no retry waits past the context deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var ctx, cancel = context.WithTimeout(t.Context(), 2500*time.Millisecond)
			defer cancel()

			// Retries at 1s and 3s; the second would start past the deadline,
			// so Do returns the last error rather than sleeping into it.
			var err, at = run(ctx, testPolicy, errRetryable, errRetryable, errRetryable)
			require.ErrorIs(t, err, errRetryable)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, seconds(0, 1), at)
		})
	})
}

func TestDoAttemptTimeout(t *testing.T) {
	// hang blocks until its attempt's context ends, the first n times.
	var hang = func(n int, calls *int) func(context.Context) error {
		return func(ctx context.Context) error {
			if *calls++; *calls > n {
				return nil
			}
			<-ctx.Done()
			return fmt.Errorf("querying: %w", ctx.Err())
		}
	}
	var policy = Policy{
		Classify: func(err error) Outcome {
			if errors.Is(err, context.DeadlineExceeded) {
				return Retryable
			}
			return Fatal
		},
		MaxAttempts:    3,
		InitialBackoff: time.Second,
		AttemptTimeout: 10 * time.Second,
	}

	t.Run("a timed-out attempt is classified", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var p = policy
			p.Classify = classifyTestErr // Fatal for context errors.

			var calls int
			var err = p.Do(t.Context(), "test op", hang(1, &calls))
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 1, calls)
		})
	})

	t.Run("each attempt gets its own deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var start = time.Now()
			var calls int
			require.NoError(t, policy.Do(t.Context(), "test op", hang(2, &calls)))
			require.Equal(t, 3, calls)
			// Two 10s timeouts plus backoffs of 1s and 2s.
			require.Equal(t, 23*time.Second, time.Since(start))
		})
	})

	t.Run("the caller's own deadline is not an attempt timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			var calls int
			var err = policy.Do(ctx, "test op", hang(1, &calls))
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 1, calls)
		})
	})

	t.Run("the attempt context ends when the attempt returns", func(t *testing.T) {
		var attemptCtx context.Context
		require.NoError(t, policy.Do(t.Context(), "test op", func(ctx context.Context) error {
			attemptCtx = ctx
			return nil
		}))
		require.ErrorIs(t, attemptCtx.Err(), context.Canceled)
	})

	t.Run("without a timeout the caller's context is passed through", func(t *testing.T) {
		var p = policy
		p.AttemptTimeout = 0

		var ctx = context.WithValue(t.Context(), struct{}{}, "v")
		require.NoError(t, p.Do(ctx, "test op", func(attemptCtx context.Context) error {
			require.Equal(t, ctx, attemptCtx)
			return nil
		}))
	})
}

func TestDoLogsEachRetry(t *testing.T) {
	var wrappedRetryable = fmt.Errorf("wrapped: %w", errRetryable)
	var hook = logtest.NewGlobal()
	t.Cleanup(hook.Reset)

	synctest.Test(t, func(t *testing.T) {
		var err, _ = run(t.Context(), testPolicy, errRetryable, wrappedRetryable)
		require.NoError(t, err)
	})

	var entries = hook.AllEntries()
	require.Len(t, entries, 2)
	for i, want := range []struct {
		err   error
		delay string
	}{
		{errRetryable, "1s"},
		{wrappedRetryable, "2s"},
	} {
		require.Equal(t, log.WarnLevel, entries[i].Level)
		require.Equal(t, "test op", entries[i].Data["operation"])
		require.Equal(t, i+1, entries[i].Data["attempt"])
		require.Equal(t, want.delay, entries[i].Data["delay"])
		require.Equal(t, want.err, entries[i].Data[log.ErrorKey])
	}
}

func TestDoQuietRetries(t *testing.T) {
	var hook = logtest.NewGlobal()
	t.Cleanup(hook.Reset)
	var level = log.GetLevel()
	log.SetLevel(log.DebugLevel)
	t.Cleanup(func() { log.SetLevel(level) })

	synctest.Test(t, func(t *testing.T) {
		var p = testPolicy
		p.QuietRetries = 2
		var err, _ = run(t.Context(), p, errRetryable, errRetryable, errRetryable)
		require.NoError(t, err)
	})

	var levels []log.Level
	for _, e := range hook.AllEntries() {
		levels = append(levels, e.Level)
	}
	require.Equal(t, []log.Level{log.DebugLevel, log.DebugLevel, log.WarnLevel}, levels)
}

func TestDoRequiresBounds(t *testing.T) {
	var noop = func(context.Context) error { return nil }
	require.PanicsWithValue(t, "retry: Policy.Classify is required", func() {
		_ = Policy{MaxAttempts: 1}.Do(t.Context(), "op", noop)
	})
	require.PanicsWithValue(t, "retry: Policy needs MaxAttempts or MaxElapsed", func() {
		_ = Policy{Classify: classifyTestErr}.Do(t.Context(), "op", noop)
	})
}
