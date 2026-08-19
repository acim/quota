package quota

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrBatchUnsupported indicates that a Store does not provide atomic batch admission.
var ErrBatchUnsupported = errors.New("quota store does not support atomic batches")

// BatchTake describes one counter mutation in an atomic batch admission.
type BatchTake struct {
	Key      string
	Amount   int64
	Capacity int64
	TTL      time.Duration
}

// BatchCounter is the result of an atomic batch admission attempt. Used has
// one entry for each requested take, in request order. When Allowed is false,
// no counter is changed and Used reports the values observed before rejection.
type BatchCounter struct {
	Allowed bool
	Used    []int64
}

// BatchStore atomically admits multiple amounts. Implementations must apply
// every take or leave every counter unchanged.
type BatchStore interface {
	Store
	TakeBatch(context.Context, []BatchTake) (BatchCounter, error)
}

// BatchDecision describes one atomic admission across multiple quota rules.
type BatchDecision struct {
	Allowed   bool
	Decisions []Decision
}

// RetryAt returns the latest reset among rules that blocked the batch. It
// returns the zero time for an allowed batch.
func (d BatchDecision) RetryAt() time.Time {
	if d.Allowed {
		return time.Time{}
	}
	var retryAt time.Time
	for _, decision := range d.Decisions {
		if !decision.Allowed && decision.ResetAt.After(retryAt) {
			retryAt = decision.ResetAt
		}
	}
	if retryAt.IsZero() {
		for _, decision := range d.Decisions {
			if decision.ResetAt.After(retryAt) {
				retryAt = decision.ResetAt
			}
		}
	}
	return retryAt
}

// ConsumeBatch admits every request atomically. A rejected batch leaves all
// counters unchanged. The configured store must implement BatchStore.
func (l *Limiter) ConsumeBatch(ctx context.Context, requests []Request) (BatchDecision, error) {
	if len(requests) == 0 {
		return BatchDecision{}, fmt.Errorf("%w: at least one quota request is required", ErrInvalidRequest)
	}

	now := l.now().UTC()
	takes := make([]BatchTake, len(requests))
	resetAt := make([]time.Time, len(requests))
	seenKeys := make(map[string]struct{}, len(requests))
	for index, request := range requests {
		if err := request.validate(); err != nil {
			return BatchDecision{}, fmt.Errorf("request %d: %w", index, err)
		}
		windowStart := now.Truncate(request.Rule.Window)
		resetAt[index] = windowStart.Add(request.Rule.Window)
		key := counterKey(request, windowStart)
		if _, exists := seenKeys[key]; exists {
			return BatchDecision{}, fmt.Errorf("%w: duplicate quota counter in batch", ErrInvalidRequest)
		}
		seenKeys[key] = struct{}{}
		takes[index] = BatchTake{
			Key:      key,
			Amount:   request.Amount,
			Capacity: request.Rule.Capacity,
			TTL:      resetAt[index].Sub(now),
		}
	}

	store, ok := l.store.(BatchStore)
	if !ok {
		return BatchDecision{}, ErrBatchUnsupported
	}
	counter, err := store.TakeBatch(ctx, takes)
	if err != nil {
		return BatchDecision{}, fmt.Errorf("take quota batch: %w", err)
	}
	if len(counter.Used) != len(requests) {
		return BatchDecision{}, fmt.Errorf("take quota batch: invalid counter result")
	}

	decisions := make([]Decision, len(requests))
	for index, request := range requests {
		allowed := counter.Allowed || (request.Amount <= request.Rule.Capacity && counter.Used[index] <= request.Rule.Capacity-request.Amount)
		decisions[index] = Decision{
			Allowed:   allowed,
			Requested: request.Amount,
			Capacity:  request.Rule.Capacity,
			Used:      counter.Used[index],
			Remaining: max(request.Rule.Capacity-counter.Used[index], 0),
			ResetAt:   resetAt[index],
		}
	}
	return BatchDecision{Allowed: counter.Allowed, Decisions: decisions}, nil
}
