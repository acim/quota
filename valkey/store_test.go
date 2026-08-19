package valkey

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
	"go.acim.net/quota"
)

func TestStoreContracts(t *testing.T) {
	t.Parallel()
	store, client := newTestStore(t)
	ctx := context.Background()
	key := uniqueKey(t, "contract")

	result, err := store.Take(ctx, key, 950, 1000, time.Hour)
	if err != nil || !result.Allowed || result.Used != 950 {
		t.Fatalf("Take(950) = %+v, %v", result, err)
	}
	result, err = store.Take(ctx, key, 100, 1000, time.Hour)
	if err != nil || result.Allowed || result.Used != 950 {
		t.Fatalf("Take(rejected) = %+v, %v", result, err)
	}
	result, err = store.Take(ctx, key, 50, 1000, time.Hour)
	if err != nil || !result.Allowed || result.Used != 1000 {
		t.Fatalf("Take(remaining) = %+v, %v", result, err)
	}
	used, err := store.Refund(ctx, key, 400)
	if err != nil || used != 600 {
		t.Fatalf("Refund(400) = %d, %v", used, err)
	}
	used, err = store.Refund(ctx, key, 700)
	if err != nil || used != 0 {
		t.Fatalf("Refund(floor) = %d, %v", used, err)
	}
	ttl, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
	if err != nil || ttl <= 0 {
		t.Fatalf("PTTL after refund = %d, %v", ttl, err)
	}
}

func TestRejectedFirstTakeCreatesNoCounter(t *testing.T) {
	t.Parallel()
	store, client := newTestStore(t)
	key := uniqueKey(t, "rejected")
	result, err := store.Take(context.Background(), key, 2, 1, time.Hour)
	if err != nil || result.Allowed || result.Used != 0 {
		t.Fatalf("Take() = %+v, %v", result, err)
	}
	exists, err := client.Do(context.Background(), client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil || exists != 0 {
		t.Fatalf("EXISTS = %d, %v", exists, err)
	}
}

func TestTakeIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(t)
	key := uniqueKey(t, "concurrent")
	const workers = 64
	start := make(chan struct{})
	results := make(chan bool, workers)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			<-start
			result, err := store.Take(context.Background(), key, 1, 25, time.Minute)
			if err != nil {
				t.Errorf("Take() error = %v", err)
				return
			}
			results <- result.Allowed
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

func TestStoreHandlesExactInt64ValuesAndValidation(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(t)
	key := uniqueKey(t, "int64")
	result, err := store.Take(context.Background(), key, math.MaxInt64, math.MaxInt64, time.Minute)
	if err != nil || !result.Allowed || result.Used != math.MaxInt64 {
		t.Fatalf("Take(MaxInt64) = %+v, %v", result, err)
	}
	result, err = store.Take(context.Background(), key, 1, math.MaxInt64, time.Minute)
	if !errors.Is(err, quota.ErrCounterOverflow) || result != (quota.Counter{}) {
		t.Fatalf("Take(overflow) = %+v, %v", result, err)
	}
	if _, err := New(nil); !errors.Is(err, quota.ErrInvalidRequest) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := store.Take(context.Background(), "", 1, 1, time.Minute); !errors.Is(err, quota.ErrInvalidRequest) {
		t.Fatalf("invalid Take() error = %v", err)
	}
	if _, err := store.Refund(context.Background(), "key", 0); !errors.Is(err, quota.ErrInvalidRequest) {
		t.Fatalf("invalid Refund() error = %v", err)
	}
	if got := ttlMilliseconds(500 * time.Microsecond); got != 1 {
		t.Fatalf("ttlMilliseconds() = %d, want 1", got)
	}
}

func newTestStore(t *testing.T) (*Store, valkeygo.Client) {
	t.Helper()
	url := os.Getenv("QUOTA_VALKEY_URL")
	if url == "" {
		t.Skip("QUOTA_VALKEY_URL is not set")
	}
	options, err := valkeygo.ParseURL(url)
	if err != nil {
		t.Fatalf("ParseURL() error = %v", err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)
	if err := client.Do(context.Background(), client.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("PING error = %v", err)
	}
	store, err := New(client)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store, client
}

func uniqueKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("quota:test:%d:%s:%s", time.Now().UnixNano(), t.Name(), suffix)
}
