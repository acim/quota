package quota

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var _ Store = (*MemoryStore)(nil)

// MemoryStore keeps counters in the current process. It is suitable for tests
// and single-instance applications.
type MemoryStore struct {
	mu          sync.Mutex
	entries     map[string]memoryEntry
	now         func() time.Time
	nextPruneAt time.Time
}

type memoryEntry struct {
	used      int64
	expiresAt time.Time
}

// NewMemoryStore creates an empty in-process store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]memoryEntry), now: time.Now}
}

// Take atomically admits amount when it fits within capacity.
func (s *MemoryStore) Take(ctx context.Context, key string, amount, capacity int64, ttl time.Duration) (Counter, error) {
	if err := validateStoreInput(key, amount, capacity, ttl); err != nil {
		return Counter{}, err
	}
	if err := ctx.Err(); err != nil {
		return Counter{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneExpiredLocked(now)
	entry, exists := s.entries[key]
	if exists && !entry.expiresAt.After(now) {
		delete(s.entries, key)
		entry, exists = memoryEntry{}, false
	}
	if amount > capacity || entry.used > capacity-amount {
		return Counter{Allowed: false, Used: entry.used}, nil
	}
	if !exists {
		entry.expiresAt = now.Add(ttl)
	}
	entry.used += amount
	s.entries[key] = entry
	return Counter{Allowed: true, Used: entry.used}, nil
}

// Refund subtracts amount and floors the counter at zero.
func (s *MemoryStore) Refund(ctx context.Context, key string, amount int64) (int64, error) {
	if key == "" || amount <= 0 {
		return 0, fmt.Errorf("%w: key and positive amount are required", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneExpiredLocked(now)
	entry, exists := s.entries[key]
	if !exists || !entry.expiresAt.After(now) {
		if exists {
			delete(s.entries, key)
		}
		return 0, nil
	}
	if entry.used <= amount {
		entry.used = 0
	} else {
		entry.used -= amount
	}
	s.entries[key] = entry
	return entry.used, nil
}

func (s *MemoryStore) pruneExpiredLocked(now time.Time) {
	if !s.nextPruneAt.IsZero() && now.Before(s.nextPruneAt) {
		return
	}
	for key, entry := range s.entries {
		if !entry.expiresAt.After(now) {
			delete(s.entries, key)
		}
	}
	s.nextPruneAt = now.Add(time.Minute)
}

func validateStoreInput(key string, amount, capacity int64, ttl time.Duration) error {
	if key == "" || amount <= 0 || capacity <= 0 || ttl <= 0 {
		return fmt.Errorf("%w: key, amount, capacity, and ttl must be positive", ErrInvalidRequest)
	}
	return nil
}
