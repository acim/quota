package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLimiterConsumesWeightedAmounts(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	request := Request{Namespace: "api", Bucket: "customer-42", Amount: 7, Rule: Rule{Capacity: 10, Window: time.Minute}}

	first, err := limiter.Consume(context.Background(), request)
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !first.Allowed || first.Used != 7 || first.Remaining != 3 || first.Requested != 7 || first.Capacity != 10 {
		t.Fatalf("first decision = %+v", first)
	}

	request.Amount = 4
	rejected, err := limiter.Consume(context.Background(), request)
	if err != nil {
		t.Fatalf("Consume(rejected) error = %v", err)
	}
	if rejected.Allowed || rejected.Used != 7 || rejected.Remaining != 3 {
		t.Fatalf("rejected decision = %+v", rejected)
	}

	request.Amount = 3
	accepted, err := limiter.Consume(context.Background(), request)
	if err != nil {
		t.Fatalf("Consume(remaining) error = %v", err)
	}
	if !accepted.Allowed || accepted.Used != 10 || accepted.Remaining != 0 {
		t.Fatalf("accepted decision = %+v", accepted)
	}
}

func TestLimiterConsumesBatchAtomically(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	requests := []Request{
		{Namespace: "forms", Bucket: "client", Amount: 1, Rule: Rule{Capacity: 2, Window: time.Minute}},
		{Namespace: "forms", Bucket: "client", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Hour}},
	}

	first, err := limiter.ConsumeBatch(context.Background(), requests)
	if err != nil || !first.Allowed || len(first.Decisions) != 2 {
		t.Fatalf("first ConsumeBatch() = %+v, %v", first, err)
	}
	rejected, err := limiter.ConsumeBatch(context.Background(), requests)
	if err != nil || rejected.Allowed || len(rejected.Decisions) != 2 {
		t.Fatalf("rejected ConsumeBatch() = %+v, %v", rejected, err)
	}
	if !rejected.Decisions[0].Allowed || rejected.Decisions[1].Allowed {
		t.Fatalf("rejected per-rule decisions = %+v, want minute allowed and hour blocked", rejected.Decisions)
	}

	minuteOnly := requests[0]
	decision, err := limiter.Consume(context.Background(), minuteOnly)
	if err != nil || !decision.Allowed || decision.Used != 2 {
		t.Fatalf("minute Consume() after rejected batch = %+v, %v; want atomic rollback", decision, err)
	}
}

func TestLimiterConsumeBatchValidatesRequestsAndStoreCapability(t *testing.T) {
	t.Parallel()
	valid := Request{Namespace: "forms", Bucket: "client", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Minute}}
	limiter := newTestLimiter(t)
	if _, err := limiter.ConsumeBatch(context.Background(), nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ConsumeBatch(nil) error = %v, want ErrInvalidRequest", err)
	}
	if _, err := limiter.ConsumeBatch(context.Background(), []Request{valid, valid}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ConsumeBatch(duplicate) error = %v, want ErrInvalidRequest", err)
	}

	nonBatchLimiter, err := New(&stubStore{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := nonBatchLimiter.ConsumeBatch(context.Background(), []Request{valid}); !errors.Is(err, ErrBatchUnsupported) {
		t.Fatalf("ConsumeBatch(non-batch store) error = %v, want ErrBatchUnsupported", err)
	}
}

func TestBatchDecisionRetryAtUsesLatestBlockingWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	decision := BatchDecision{Allowed: false, Decisions: []Decision{
		{Requested: 1, Capacity: 5, Used: 5, ResetAt: now.Add(time.Minute)},
		{Allowed: true, Requested: 1, Capacity: 30, Used: 29, ResetAt: now.Add(time.Hour)},
		{Requested: 1, Capacity: 100, Used: 100, ResetAt: now.Add(24 * time.Hour)},
	}}
	if got, want := decision.RetryAt(), now.Add(24*time.Hour); !got.Equal(want) {
		t.Fatalf("RetryAt() = %v, want %v", got, want)
	}
	decision.Allowed = true
	if got := decision.RetryAt(); !got.IsZero() {
		t.Fatalf("allowed RetryAt() = %v, want zero", got)
	}
	decision = BatchDecision{Allowed: false, Decisions: []Decision{
		{Allowed: true, Requested: 1, Capacity: 5, Used: 0, ResetAt: now.Add(time.Minute)},
		{Allowed: true, Requested: 1, Capacity: 30, Used: 0, ResetAt: now.Add(time.Hour)},
	}}
	if got, want := decision.RetryAt(), now.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("fallback RetryAt() = %v, want %v", got, want)
	}
}

func TestLimiterConsumeBatchPropagatesStoreErrorsAndInvalidResults(t *testing.T) {
	t.Parallel()
	request := Request{Namespace: "forms", Bucket: "client", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Minute}}
	storeErr := errors.New("store unavailable")
	for name, store := range map[string]*stubBatchStore{
		"store error":    {batchErr: storeErr},
		"invalid result": {batchCounter: BatchCounter{Allowed: true}},
	} {
		t.Run(name, func(t *testing.T) {
			limiter, err := New(store)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			_, err = limiter.ConsumeBatch(context.Background(), []Request{request})
			if name == "store error" && !errors.Is(err, storeErr) {
				t.Fatalf("ConsumeBatch() error = %v, want store error", err)
			}
			if name == "invalid result" && err == nil {
				t.Fatal("ConsumeBatch() error = nil, want invalid result error")
			}
		})
	}
}

func TestMemoryStoreBatchHonorsCancellationAndValidation(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	valid := BatchTake{Key: "minute", Amount: 1, Capacity: 1, TTL: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.TakeBatch(ctx, []BatchTake{valid}); !errors.Is(err, context.Canceled) {
		t.Fatalf("TakeBatch(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := store.TakeBatch(context.Background(), nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("TakeBatch(nil) error = %v, want ErrInvalidRequest", err)
	}
	if _, err := store.TakeBatch(context.Background(), []BatchTake{valid, valid}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("TakeBatch(duplicate) error = %v, want ErrInvalidRequest", err)
	}
}

func TestMemoryStoreBatchDoesNotChargeWhenCanceledWhileWaitingForLock(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	takes := []BatchTake{
		{Key: "minute", Amount: 1, Capacity: 2, TTL: time.Minute},
		{Key: "hour", Amount: 1, Capacity: 2, TTL: time.Hour},
	}
	if result, err := store.TakeBatch(context.Background(), takes); err != nil || !result.Allowed {
		t.Fatalf("prime TakeBatch() = %+v, %v", result, err)
	}

	store.mu.Lock()
	base, cancel := context.WithCancel(context.Background())
	ctx := &observedContext{Context: base, errChecked: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := store.TakeBatch(ctx, takes)
		result <- err
	}()
	<-ctx.errChecked
	cancel()
	store.mu.Unlock()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("TakeBatch() error = %v, want context.Canceled", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, take := range takes {
		if got := store.entries[take.Key].used; got != 1 {
			t.Fatalf("counter %q used = %d, want 1", take.Key, got)
		}
	}
}

func TestMemoryStoreBatchCountersExpire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	takes := []BatchTake{
		{Key: "minute", Amount: 1, Capacity: 1, TTL: time.Second},
		{Key: "hour", Amount: 1, Capacity: 1, TTL: time.Second},
	}
	if result, err := store.TakeBatch(context.Background(), takes); err != nil || !result.Allowed {
		t.Fatalf("first TakeBatch() = %+v, %v", result, err)
	}
	if result, err := store.TakeBatch(context.Background(), takes); err != nil || result.Allowed {
		t.Fatalf("TakeBatch() before expiry = %+v, %v", result, err)
	}

	now = now.Add(2 * time.Second)
	if result, err := store.TakeBatch(context.Background(), takes); err != nil || !result.Allowed {
		t.Fatalf("TakeBatch() after expiry = %+v, %v", result, err)
	}
}

func TestLimiterUsesIsolatedUTCAlignedWindows(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	now := time.Date(2026, 8, 19, 11, 59, 30, 0, time.FixedZone("example", 2*60*60))
	limiter.now = func() time.Time { return now }
	request := Request{Namespace: "jobs", Bucket: "tenant", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Hour}}

	first, err := limiter.Consume(context.Background(), request)
	if err != nil || !first.Allowed {
		t.Fatalf("Consume() = %+v, %v", first, err)
	}
	wantReset := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	if !first.ResetAt.Equal(wantReset) {
		t.Fatalf("ResetAt = %v, want %v", first.ResetAt, wantReset)
	}

	other := request
	other.Namespace = "exports"
	decision, err := limiter.Consume(context.Background(), other)
	if err != nil || !decision.Allowed {
		t.Fatalf("isolated namespace Consume() = %+v, %v", decision, err)
	}
	other.Namespace = request.Namespace
	other.Bucket = "another-tenant"
	decision, err = limiter.Consume(context.Background(), other)
	if err != nil || !decision.Allowed {
		t.Fatalf("isolated bucket Consume() = %+v, %v", decision, err)
	}

	now = now.Add(31 * time.Second)
	decision, err = limiter.Consume(context.Background(), request)
	if err != nil || !decision.Allowed || decision.Used != 1 {
		t.Fatalf("next-window Consume() = %+v, %v", decision, err)
	}
}

func TestLimiterUsesConfiguredClock(t *testing.T) {
	t.Parallel()
	now := time.Unix(5, 250_000_000).UTC()
	limiter, err := New(NewMemoryStore(), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	decision, err := limiter.Consume(context.Background(), Request{
		Namespace: "api",
		Bucket:    "customer-42",
		Amount:    1,
		Rule:      Rule{Capacity: 1, Window: 1500 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	wantReset := time.Unix(6, 0).UTC()
	if !decision.ResetAt.Equal(wantReset) {
		t.Fatalf("ResetAt = %v, want %v", decision.ResetAt, wantReset)
	}
}

func TestLimiterRejectsInvalidOptions(t *testing.T) {
	t.Parallel()
	for name, option := range map[string]Option{
		"nil clock":  WithClock(nil),
		"nil option": nil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(NewMemoryStore(), option); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("New() error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestReservationCommitAndRelease(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	request := Request{Namespace: "work", Bucket: "account", Amount: 80, Rule: Rule{Capacity: 100, Window: time.Hour}}

	decision, reservation, err := limiter.Reserve(context.Background(), request)
	if err != nil || !decision.Allowed || reservation == nil || reservation.Reserved() != 80 {
		t.Fatalf("Reserve() = %+v, %v, %v", decision, reservation, err)
	}
	if err := reservation.Commit(context.Background(), 30); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := reservation.Commit(context.Background(), 30); err != nil {
		t.Fatalf("repeated Commit() error = %v", err)
	}
	if err := reservation.Commit(context.Background(), 20); !errors.Is(err, ErrReservationFinalized) {
		t.Fatalf("conflicting Commit() error = %v", err)
	}

	request.Amount = 70
	decision, released, err := limiter.Reserve(context.Background(), request)
	if err != nil || !decision.Allowed || released == nil {
		t.Fatalf("second Reserve() = %+v, %v, %v", decision, released, err)
	}
	if err := released.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := released.Release(context.Background()); err != nil {
		t.Fatalf("repeated Release() error = %v", err)
	}

	request.Amount = 70
	final, err := limiter.Consume(context.Background(), request)
	if err != nil || !final.Allowed || final.Used != 100 {
		t.Fatalf("Consume() after reconciliation = %+v, %v", final, err)
	}
}

func TestReservationRejectsInvalidActualAmount(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	request := Request{Namespace: "work", Bucket: "account", Amount: 10, Rule: Rule{Capacity: 10, Window: time.Hour}}
	_, reservation, err := limiter.Reserve(context.Background(), request)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if err := reservation.Commit(context.Background(), -1); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Commit(-1) error = %v", err)
	}
	if err := reservation.Commit(context.Background(), 11); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Commit(11) error = %v", err)
	}
	if err := reservation.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestLimiterValidatesRequests(t *testing.T) {
	t.Parallel()
	valid := Request{Namespace: "api", Bucket: "user", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Minute}}
	tests := []struct {
		name   string
		mutate func(*Request)
	}{
		{name: "namespace", mutate: func(r *Request) { r.Namespace = " " }},
		{name: "bucket", mutate: func(r *Request) { r.Bucket = "" }},
		{name: "amount", mutate: func(r *Request) { r.Amount = 0 }},
		{name: "capacity", mutate: func(r *Request) { r.Rule.Capacity = -1 }},
		{name: "window", mutate: func(r *Request) { r.Rule.Window = 0 }},
	}
	limiter := newTestLimiter(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if _, err := limiter.Consume(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Consume() error = %v", err)
			}
		})
	}
	if _, err := New(nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("New(nil) error = %v", err)
	}
}

func TestLimiterPropagatesStoreErrorsAndRejectedReservations(t *testing.T) {
	t.Parallel()
	storeErr := errors.New("store unavailable")
	limiter, err := New(&stubStore{takeErr: storeErr})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := Request{Namespace: "api", Bucket: "user", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Minute}}
	if _, err := limiter.Consume(context.Background(), request); !errors.Is(err, storeErr) {
		t.Fatalf("Consume() error = %v", err)
	}

	limiter, err = New(&stubStore{counter: Counter{Allowed: false, Used: 1}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	decision, reservation, err := limiter.Reserve(context.Background(), request)
	if err != nil || decision.Allowed || reservation != nil {
		t.Fatalf("Reserve() = %+v, %v, %v", decision, reservation, err)
	}
}

func TestReservationRetriesRefundFailures(t *testing.T) {
	t.Parallel()
	storeErr := errors.New("temporary failure")
	store := &stubStore{counter: Counter{Allowed: true, Used: 10}, refundErr: storeErr}
	limiter, err := New(store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := Request{Namespace: "work", Bucket: "account", Amount: 10, Rule: Rule{Capacity: 10, Window: time.Hour}}
	_, reservation, err := limiter.Reserve(context.Background(), request)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if err := reservation.Commit(context.Background(), 4); !errors.Is(err, storeErr) {
		t.Fatalf("Commit() error = %v", err)
	}
	store.refundErr = nil
	if err := reservation.Commit(context.Background(), 4); err != nil {
		t.Fatalf("retried Commit() error = %v", err)
	}
	if store.refunded != 6 {
		t.Fatalf("refunded = %d, want 6", store.refunded)
	}
	if err := reservation.Release(context.Background()); !errors.Is(err, ErrReservationFinalized) {
		t.Fatalf("Release() after Commit() error = %v", err)
	}

	store.refundErr = storeErr
	_, reservation, err = limiter.Reserve(context.Background(), request)
	if err != nil {
		t.Fatalf("second Reserve() error = %v", err)
	}
	if err := reservation.Release(context.Background()); !errors.Is(err, storeErr) {
		t.Fatalf("Release() error = %v", err)
	}
	store.refundErr = nil
	if err := reservation.Release(context.Background()); err != nil {
		t.Fatalf("retried Release() error = %v", err)
	}
}

func TestMemoryStoreExpiresAndPrunesCounters(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore()
	store.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := store.Take(ctx, "expired", 1, 1, time.Second); err != nil {
		t.Fatalf("Take(expired) error = %v", err)
	}
	if _, err := store.Take(ctx, "active", 1, 2, time.Hour); err != nil {
		t.Fatalf("Take(active) error = %v", err)
	}
	now = now.Add(time.Minute)
	if used, err := store.Refund(ctx, "expired", 1); err != nil || used != 0 {
		t.Fatalf("Refund(expired) = %d, %v", used, err)
	}
	if result, err := store.Take(ctx, "expired", 1, 1, time.Second); err != nil || !result.Allowed || result.Used != 1 {
		t.Fatalf("Take(reset) = %+v, %v", result, err)
	}
	store.mu.Lock()
	_, activeExists := store.entries["active"]
	store.mu.Unlock()
	if !activeExists {
		t.Fatal("pruning removed active counter")
	}
}

func TestMemoryStoreAdmissionIsAtomic(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	request := Request{Namespace: "concurrency", Bucket: "shared", Amount: 1, Rule: Rule{Capacity: 25, Window: time.Minute}}
	const workers = 64
	start := make(chan struct{})
	results := make(chan bool, workers)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			<-start
			decision, err := limiter.Consume(context.Background(), request)
			if err != nil {
				t.Errorf("Consume() error = %v", err)
				return
			}
			results <- decision.Allowed
		})
	}
	close(start)
	group.Wait()
	close(results)
	accepted := 0
	for allowed := range results {
		if allowed {
			accepted++
		}
	}
	if accepted != 25 {
		t.Fatalf("accepted = %d, want 25", accepted)
	}
}

func TestMemoryStoreBatchAdmissionIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(t)
	requests := []Request{
		{Namespace: "batch-concurrency", Bucket: "shared", Amount: 1, Rule: Rule{Capacity: 25, Window: time.Minute}},
		{Namespace: "batch-concurrency", Bucket: "shared", Amount: 1, Rule: Rule{Capacity: 10, Window: time.Hour}},
	}
	const workers = 64
	start := make(chan struct{})
	results := make(chan bool, workers)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			<-start
			decision, err := limiter.ConsumeBatch(context.Background(), requests)
			if err != nil {
				t.Errorf("ConsumeBatch() error = %v", err)
				return
			}
			results <- decision.Allowed
		})
	}
	close(start)
	group.Wait()
	close(results)
	accepted := 0
	for allowed := range results {
		if allowed {
			accepted++
		}
	}
	if accepted != 10 {
		t.Fatalf("accepted batches = %d, want 10", accepted)
	}

	minute := requests[0]
	minute.Amount = 15
	decision, err := limiter.Consume(context.Background(), minute)
	if err != nil || !decision.Allowed || decision.Used != 25 {
		t.Fatalf("minute Consume() after concurrent batches = %+v, %v; want no partial charges", decision, err)
	}
}

func TestMemoryStoreHonorsCancellationAndValidation(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Take(ctx, "key", 1, 1, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("Take() error = %v", err)
	}
	if _, err := store.Refund(ctx, "key", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Refund() error = %v", err)
	}
	if _, err := store.Take(context.Background(), "", 1, 1, time.Minute); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid Take() error = %v", err)
	}
	if _, err := store.Refund(context.Background(), "key", 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid Refund() error = %v", err)
	}
}

func newTestLimiter(t *testing.T) *Limiter {
	t.Helper()
	limiter, err := New(NewMemoryStore())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return limiter
}

type stubStore struct {
	counter   Counter
	takeErr   error
	refundErr error
	refunded  int64
}

type stubBatchStore struct {
	stubStore
	batchCounter BatchCounter
	batchErr     error
}

type observedContext struct {
	context.Context
	errChecked chan struct{}
	once       sync.Once
}

func (c *observedContext) Err() error {
	c.once.Do(func() { close(c.errChecked) })
	return c.Context.Err()
}

func (s *stubBatchStore) TakeBatch(context.Context, []BatchTake) (BatchCounter, error) {
	return s.batchCounter, s.batchErr
}

func (s *stubStore) Take(context.Context, string, int64, int64, time.Duration) (Counter, error) {
	if s.takeErr != nil {
		return Counter{}, fmt.Errorf("take: %w", s.takeErr)
	}
	return s.counter, nil
}

func (s *stubStore) Refund(_ context.Context, _ string, amount int64) (int64, error) {
	if s.refundErr != nil {
		return 0, fmt.Errorf("refund: %w", s.refundErr)
	}
	s.refunded += amount
	return 0, nil
}
