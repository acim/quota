package quota

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Rule limits the admitted amount in one aligned fixed window.
type Rule struct {
	Capacity int64
	Window   time.Duration
}

// Request identifies an amount to admit against a rule.
type Request struct {
	Namespace string
	Bucket    string
	Amount    int64
	Rule      Rule
}

// Decision describes the state observed after an admission attempt.
type Decision struct {
	Allowed   bool
	Requested int64
	Capacity  int64
	Used      int64
	Remaining int64
	ResetAt   time.Time
}

// Limiter applies quota rules using a Store.
type Limiter struct {
	store Store
	now   func() time.Time
}

// Option configures a Limiter.
type Option interface {
	apply(*Limiter) error
}

type optionFunc func(*Limiter) error

func (option optionFunc) apply(limiter *Limiter) error {
	return option(limiter)
}

// WithClock configures the clock used to align quota windows.
func WithClock(now func() time.Time) Option {
	return optionFunc(func(limiter *Limiter) error {
		if now == nil {
			return fmt.Errorf("%w: clock is required", ErrInvalidRequest)
		}
		limiter.now = now
		return nil
	})
}

// New creates a Limiter backed by store.
func New(store Store, options ...Option) (*Limiter, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidRequest)
	}
	limiter := &Limiter{store: store, now: time.Now}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: option is required", ErrInvalidRequest)
		}
		if err := option.apply(limiter); err != nil {
			return nil, err
		}
	}
	return limiter, nil
}

// Consume admits an amount that is known before work begins.
func (l *Limiter) Consume(ctx context.Context, request Request) (Decision, error) {
	decision, _, err := l.admit(ctx, request)
	return decision, err
}

// Reserve admits an upper bound before work begins. The returned reservation
// can later retain the actual amount or refund the entire amount.
func (l *Limiter) Reserve(ctx context.Context, request Request) (Decision, *Reservation, error) {
	decision, key, err := l.admit(ctx, request)
	if err != nil || !decision.Allowed {
		return decision, nil, err
	}
	return decision, &Reservation{store: l.store, key: key, reserved: request.Amount}, nil
}

func (l *Limiter) admit(ctx context.Context, request Request) (Decision, string, error) {
	if err := request.validate(); err != nil {
		return Decision{}, "", err
	}

	now := l.now().UTC()
	windowStart := now.Truncate(request.Rule.Window)
	resetAt := windowStart.Add(request.Rule.Window)
	key := counterKey(request, windowStart)
	counter, err := l.store.Take(ctx, key, request.Amount, request.Rule.Capacity, resetAt.Sub(now))
	if err != nil {
		return Decision{}, "", fmt.Errorf("take quota: %w", err)
	}

	remaining := max(request.Rule.Capacity-counter.Used, 0)
	return Decision{
		Allowed:   counter.Allowed,
		Requested: request.Amount,
		Capacity:  request.Rule.Capacity,
		Used:      counter.Used,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, key, nil
}

func (r Request) validate() error {
	if strings.TrimSpace(r.Namespace) == "" {
		return fmt.Errorf("%w: namespace is required", ErrInvalidRequest)
	}
	if strings.TrimSpace(r.Bucket) == "" {
		return fmt.Errorf("%w: bucket is required", ErrInvalidRequest)
	}
	if r.Amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidRequest)
	}
	if r.Rule.Capacity <= 0 {
		return fmt.Errorf("%w: capacity must be positive", ErrInvalidRequest)
	}
	if r.Rule.Window <= 0 {
		return fmt.Errorf("%w: window must be positive", ErrInvalidRequest)
	}
	return nil
}

func counterKey(request Request, windowStart time.Time) string {
	return "quota:v1:" + digest(request.Namespace) + ":" + digest(request.Bucket) + ":" +
		strconv.FormatInt(int64(request.Rule.Window), 10) + ":" +
		strconv.FormatInt(windowStart.UnixNano(), 10)
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
