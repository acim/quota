package quota

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

func TestValkeyStoreContracts(t *testing.T) {
	t.Parallel()
	store, client := newTestValkeyStore(t)
	ctx := context.Background()
	key := uniqueValkeyKey(t, "contract")

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

func TestValkeyStoreRejectedFirstTakeCreatesNoCounter(t *testing.T) {
	t.Parallel()
	store, client := newTestValkeyStore(t)
	key := uniqueValkeyKey(t, "rejected")
	result, err := store.Take(context.Background(), key, 2, 1, time.Hour)
	if err != nil || result.Allowed || result.Used != 0 {
		t.Fatalf("Take() = %+v, %v", result, err)
	}
	exists, err := client.Do(context.Background(), client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil || exists != 0 {
		t.Fatalf("EXISTS = %d, %v", exists, err)
	}
}

func TestValkeyStoreTakeIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	store, _ := newTestValkeyStore(t)
	key := uniqueValkeyKey(t, "concurrent")
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

func TestValkeyStoreHandlesExactInt64ValuesAndValidation(t *testing.T) {
	t.Parallel()
	store, _ := newTestValkeyStore(t)
	key := uniqueValkeyKey(t, "int64")
	result, err := store.Take(context.Background(), key, math.MaxInt64, math.MaxInt64, time.Minute)
	if err != nil || !result.Allowed || result.Used != math.MaxInt64 {
		t.Fatalf("Take(MaxInt64) = %+v, %v", result, err)
	}
	result, err = store.Take(context.Background(), key, 1, math.MaxInt64, time.Minute)
	if err != nil || result.Allowed || result.Used != math.MaxInt64 {
		t.Fatalf("Take(over capacity) = %+v, %v", result, err)
	}
	if _, err := NewValkeyStore(nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("NewValkeyStore(nil) error = %v", err)
	}
	if _, err := store.Take(context.Background(), "", 1, 1, time.Minute); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid Take() error = %v", err)
	}
	if _, err := store.Refund(context.Background(), "key", 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid Refund() error = %v", err)
	}
	if got := ttlMilliseconds(500 * time.Microsecond); got != 1 {
		t.Fatalf("ttlMilliseconds() = %d, want 1", got)
	}
}

func TestValkeyStoreUsesExactInt64Comparisons(t *testing.T) {
	t.Parallel()
	store, client := newTestValkeyStore(t)
	ctx := context.Background()
	const aboveExactFloat = int64(1 << 53)

	t.Run("rejects one above limit without rounding or mutation", func(t *testing.T) {
		key := uniqueValkeyKey(t, "exact-reject")
		result, err := store.Take(ctx, key, aboveExactFloat, aboveExactFloat, time.Minute)
		if err != nil || !result.Allowed || result.Used != aboveExactFloat {
			t.Fatalf("Take(2^53) = %+v, %v", result, err)
		}
		initialTTL, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
		if err != nil || initialTTL <= 0 {
			t.Fatalf("initial PTTL = %d, %v", initialTTL, err)
		}

		result, err = store.Take(ctx, key, 1, aboveExactFloat, time.Hour)
		if err != nil || result.Allowed || result.Used != aboveExactFloat {
			t.Fatalf("Take(2^53+1 > 2^53) = %+v, %v", result, err)
		}
		value, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
		if err != nil || value != strconv.FormatInt(aboveExactFloat, 10) {
			t.Fatalf("counter after denial = %q, %v; want %d", value, err, aboveExactFloat)
		}
		remainingTTL, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
		if err != nil || remainingTTL <= 0 || remainingTTL > initialTTL {
			t.Fatalf("PTTL after denial = %d, %v; want positive and <= %d", remainingTTL, err, initialTTL)
		}
	})

	t.Run("accepts exact value above float precision", func(t *testing.T) {
		key := uniqueValkeyKey(t, "exact-accept")
		capacity := aboveExactFloat + 1
		result, err := store.Take(ctx, key, aboveExactFloat, capacity, time.Minute)
		if err != nil || !result.Allowed || result.Used != aboveExactFloat {
			t.Fatalf("Take(2^53) = %+v, %v", result, err)
		}
		result, err = store.Take(ctx, key, 1, capacity, time.Minute)
		if err != nil || !result.Allowed || result.Used != capacity {
			t.Fatalf("Take(exact 2^53+1) = %+v, %v", result, err)
		}
	})

	t.Run("accepts and rejects exactly near MaxInt64", func(t *testing.T) {
		key := uniqueValkeyKey(t, "exact-max")
		result, err := store.Take(ctx, key, math.MaxInt64-2, math.MaxInt64, time.Minute)
		if err != nil || !result.Allowed || result.Used != math.MaxInt64-2 {
			t.Fatalf("Take(MaxInt64-2) = %+v, %v", result, err)
		}
		result, err = store.Take(ctx, key, 1, math.MaxInt64-1, time.Minute)
		if err != nil || !result.Allowed || result.Used != math.MaxInt64-1 {
			t.Fatalf("Take(MaxInt64-1) = %+v, %v", result, err)
		}
		result, err = store.Take(ctx, key, 1, math.MaxInt64-1, time.Minute)
		if err != nil || result.Allowed || result.Used != math.MaxInt64-1 {
			t.Fatalf("reject above MaxInt64-1 = %+v, %v", result, err)
		}
		result, err = store.Take(ctx, key, 1, math.MaxInt64, time.Minute)
		if err != nil || !result.Allowed || result.Used != math.MaxInt64 {
			t.Fatalf("Take(MaxInt64) = %+v, %v", result, err)
		}
		result, err = store.Take(ctx, key, 1, math.MaxInt64, time.Minute)
		if err != nil || result.Allowed || result.Used != math.MaxInt64 {
			t.Fatalf("reject overflow at MaxInt64 = %+v, %v", result, err)
		}
	})
}

func newTestValkeyStore(t *testing.T) (*ValkeyStore, valkey.Client) {
	t.Helper()
	url := os.Getenv("QUOTA_VALKEY_URL")
	if url == "" {
		t.Skip("QUOTA_VALKEY_URL is not set")
	}
	options, err := valkey.ParseURL(url)
	if err != nil {
		t.Fatalf("ParseURL() error = %v", err)
	}
	client, err := valkey.NewClient(options)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)
	if err := client.Do(context.Background(), client.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("PING error = %v", err)
	}
	store, err := NewValkeyStore(client)
	if err != nil {
		t.Fatalf("NewValkeyStore() error = %v", err)
	}
	return store, client
}

func uniqueValkeyKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("quota:test:%d:%s:%s", time.Now().UnixNano(), t.Name(), suffix)
}
