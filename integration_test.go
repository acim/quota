package quota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestValkeyLimiterConsumesWeightedAmounts(t *testing.T) {
	t.Parallel()
	limiter := newTestValkeyLimiter(t)
	request := Request{
		Namespace: uniqueValkeyKey(t, "consume"),
		Bucket:    "account",
		Amount:    7,
		Rule:      Rule{Capacity: 10, Window: time.Minute},
	}

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

func TestValkeyLimiterConsumesBatchAtomically(t *testing.T) {
	t.Parallel()
	limiter := newTestValkeyLimiter(t)
	namespace := uniqueValkeyKey(t, "batch")
	requests := []Request{
		{Namespace: namespace, Bucket: "client", Amount: 1, Rule: Rule{Capacity: 2, Window: time.Minute}},
		{Namespace: namespace, Bucket: "client", Amount: 1, Rule: Rule{Capacity: 1, Window: time.Hour}},
	}

	first, err := limiter.ConsumeBatch(context.Background(), requests)
	if err != nil || !first.Allowed || len(first.Decisions) != 2 {
		t.Fatalf("first ConsumeBatch() = %+v, %v", first, err)
	}
	rejected, err := limiter.ConsumeBatch(context.Background(), requests)
	if err != nil || rejected.Allowed || len(rejected.Decisions) != 2 {
		t.Fatalf("rejected ConsumeBatch() = %+v, %v", rejected, err)
	}

	decision, err := limiter.Consume(context.Background(), requests[0])
	if err != nil || !decision.Allowed || decision.Used != 2 {
		t.Fatalf("minute Consume() after rejected batch = %+v, %v; want atomic rollback", decision, err)
	}
}

func TestValkeyLimiterIsolatesNamespacesBucketsAndWindows(t *testing.T) {
	t.Parallel()
	limiter := newTestValkeyLimiter(t)
	now := time.Unix(5, 0)
	limiter.now = func() time.Time { return now }
	request := Request{
		Namespace: uniqueValkeyKey(t, "namespace"),
		Bucket:    "account-1",
		Amount:    1,
		Rule:      Rule{Capacity: 1, Window: time.Minute},
	}

	first, err := limiter.Consume(context.Background(), request)
	if err != nil || !first.Allowed {
		t.Fatalf("Consume() = %+v, %v", first, err)
	}
	if want := time.Unix(60, 0).UTC(); !first.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %v, want %v", first.ResetAt, want)
	}

	otherNamespace := request
	otherNamespace.Namespace += ":other"
	if decision, err := limiter.Consume(context.Background(), otherNamespace); err != nil || !decision.Allowed {
		t.Fatalf("other namespace Consume() = %+v, %v", decision, err)
	}
	otherBucket := request
	otherBucket.Bucket = "account-2"
	if decision, err := limiter.Consume(context.Background(), otherBucket); err != nil || !decision.Allowed {
		t.Fatalf("other bucket Consume() = %+v, %v", decision, err)
	}

	now = time.Unix(60, 0)
	if decision, err := limiter.Consume(context.Background(), request); err != nil || !decision.Allowed || decision.Used != 1 {
		t.Fatalf("next window Consume() = %+v, %v", decision, err)
	}
}

func TestValkeyLimiterReservationsReconcileActualUsage(t *testing.T) {
	t.Parallel()
	limiter := newTestValkeyLimiter(t)
	request := Request{
		Namespace: uniqueValkeyKey(t, "reservation"),
		Bucket:    "account",
		Amount:    80,
		Rule:      Rule{Capacity: 100, Window: time.Hour},
	}

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
	if err := reservation.Release(context.Background()); !errors.Is(err, ErrReservationFinalized) {
		t.Fatalf("Release() after Commit() error = %v", err)
	}

	request.Amount = 71
	rejected, rejectedReservation, err := limiter.Reserve(context.Background(), request)
	if err != nil || rejected.Allowed || rejected.Used != 30 || rejectedReservation != nil {
		t.Fatalf("rejected Reserve() = %+v, %v, %v", rejected, rejectedReservation, err)
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

	final, err := limiter.Consume(context.Background(), request)
	if err != nil || !final.Allowed || final.Used != 100 {
		t.Fatalf("Consume() after reconciliation = %+v, %v", final, err)
	}
}

func TestValkeyLimiterReservationValidationDoesNotFinalize(t *testing.T) {
	t.Parallel()
	limiter := newTestValkeyLimiter(t)
	request := Request{
		Namespace: uniqueValkeyKey(t, "reservation-validation"),
		Bucket:    "account",
		Amount:    10,
		Rule:      Rule{Capacity: 10, Window: time.Hour},
	}
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
		t.Fatalf("Release() after invalid commits error = %v", err)
	}

	request.Amount = 10
	if decision, err := limiter.Consume(context.Background(), request); err != nil || !decision.Allowed || decision.Used != 10 {
		t.Fatalf("Consume() after release = %+v, %v", decision, err)
	}
}

func TestValkeyLimiterSharesQuotaAcrossClientsConcurrently(t *testing.T) {
	t.Parallel()
	first := newTestValkeyLimiter(t)
	second := newTestValkeyLimiter(t)
	request := Request{
		Namespace: uniqueValkeyKey(t, "distributed"),
		Bucket:    "shared",
		Amount:    1,
		Rule:      Rule{Capacity: 25, Window: time.Minute},
	}
	const workers = 64
	start := make(chan struct{})
	results := make(chan bool, workers)
	var group sync.WaitGroup
	for index := range workers {
		limiter := first
		if index%2 == 1 {
			limiter = second
		}
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

func TestValkeyLimiterConsumesBatchAtomicallyAcrossClients(t *testing.T) {
	t.Parallel()
	first := newTestValkeyLimiter(t)
	second := newTestValkeyLimiter(t)
	namespace := uniqueValkeyKey(t, "batch-distributed")
	requests := []Request{
		{Namespace: namespace, Bucket: "shared", Amount: 1, Rule: Rule{Capacity: 25, Window: time.Minute}},
		{Namespace: namespace, Bucket: "shared", Amount: 1, Rule: Rule{Capacity: 10, Window: time.Hour}},
	}
	const workers = 64
	start := make(chan struct{})
	results := make(chan bool, workers)
	var group sync.WaitGroup
	for index := range workers {
		limiter := first
		if index%2 == 1 {
			limiter = second
		}
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
	decision, err := first.Consume(context.Background(), minute)
	if err != nil || !decision.Allowed || decision.Used != 25 {
		t.Fatalf("minute Consume() after concurrent batches = %+v, %v; want no partial charges", decision, err)
	}
}

func TestValkeyLimiterPropagatesCancellation(t *testing.T) {
	t.Parallel()
	limiter := newTestValkeyLimiter(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := Request{
		Namespace: uniqueValkeyKey(t, "cancellation"),
		Bucket:    "account",
		Amount:    1,
		Rule:      Rule{Capacity: 1, Window: time.Minute},
	}
	if _, err := limiter.Consume(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume() error = %v, want context.Canceled", err)
	}
}

func TestValkeyStoreExpiresCounters(t *testing.T) {
	t.Parallel()
	store, _ := newTestValkeyStore(t)
	ctx := context.Background()
	key := uniqueValkeyKey(t, "expiry")
	if result, err := store.Take(ctx, key, 1, 1, 20*time.Millisecond); err != nil || !result.Allowed {
		t.Fatalf("first Take() = %+v, %v", result, err)
	}
	if result, err := store.Take(ctx, key, 1, 1, time.Minute); err != nil || result.Allowed {
		t.Fatalf("Take() before expiry = %+v, %v", result, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		result, err := store.Take(ctx, key, 1, 1, time.Minute)
		if err != nil {
			t.Fatalf("Take() while awaiting expiry error = %v", err)
		}
		if result.Allowed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("counter did not expire within 2 seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newTestValkeyLimiter(t *testing.T) *Limiter {
	t.Helper()
	store, _ := newTestValkeyStore(t)
	limiter, err := New(store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return limiter
}
