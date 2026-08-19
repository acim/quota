package quota

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidRequest indicates that a request, rule, or store operation has
	// a missing or non-positive value.
	ErrInvalidRequest = errors.New("invalid quota request")
	// ErrCounterOverflow indicates that a backend counter exceeded int64.
	ErrCounterOverflow = errors.New("quota counter overflow")
	// ErrReservationFinalized indicates an attempt to finalize a reservation
	// with a result that conflicts with its completed result.
	ErrReservationFinalized = errors.New("quota reservation already finalized")
)

// Counter is the result of an atomic store admission attempt.
type Counter struct {
	Allowed bool
	Used    int64
}

// Store atomically admits amounts and refunds previously admitted amounts.
// Implementations must leave a counter unchanged when Take rejects an amount.
// Refund must floor the counter at zero and preserve its original expiry.
type Store interface {
	Take(ctx context.Context, key string, amount, capacity int64, ttl time.Duration) (Counter, error)
	Refund(ctx context.Context, key string, amount int64) (int64, error)
}
