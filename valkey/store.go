// Package valkey provides a distributed quota store backed by Valkey.
package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
	"go.acim.net/quota"
)

var _ quota.Store = (*Store)(nil)

// Store uses a caller-owned Valkey client. Closing the store does not close the
// client.
type Store struct {
	client valkeygo.Client
}

var takeScript = valkeygo.NewLuaScript(`
local function decimal_greater(left, right)
  if string.len(left) ~= string.len(right) then
    return string.len(left) > string.len(right)
  end
  return left > right
end

local existed = redis.call('EXISTS', KEYS[1])
redis.call('INCRBY', KEYS[1], ARGV[1])
local count = redis.call('GET', KEYS[1])
if decimal_greater(count, ARGV[2]) then
  local previous
  if existed == 0 then
    redis.call('DEL', KEYS[1])
    previous = '0'
  else
    redis.call('DECRBY', KEYS[1], ARGV[1])
    previous = redis.call('GET', KEYS[1])
  end
  return {previous, 0}
end
if existed == 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return {count, 1}
`)

var refundScript = valkeygo.NewLuaScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return 0
end
local amount = ARGV[1]
local current_is_at_most_amount = string.len(current) < string.len(amount)
  or (string.len(current) == string.len(amount) and current <= amount)
if current_is_at_most_amount then
  redis.call('SET', KEYS[1], 0, 'KEEPTTL')
  return 0
end
return redis.call('DECRBY', KEYS[1], ARGV[1])
`)

// New creates a store using client. The caller retains ownership of client.
func New(client valkeygo.Client) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: valkey client is required", quota.ErrInvalidRequest)
	}
	return &Store{client: client}, nil
}

// Take atomically admits amount when it fits within capacity.
func (s *Store) Take(ctx context.Context, key string, amount, capacity int64, ttl time.Duration) (quota.Counter, error) {
	if key == "" || amount <= 0 || capacity <= 0 || ttl <= 0 {
		return quota.Counter{}, fmt.Errorf("%w: key, amount, capacity, and ttl must be positive", quota.ErrInvalidRequest)
	}
	result, err := takeScript.Exec(
		ctx,
		s.client,
		[]string{key},
		[]string{strconv.FormatInt(amount, 10), strconv.FormatInt(capacity, 10), strconv.FormatInt(ttlMilliseconds(ttl), 10)},
	).AsIntSlice()
	if err != nil {
		return quota.Counter{}, operationError("take quota", err)
	}
	if len(result) != 2 {
		return quota.Counter{}, errors.New("take quota: invalid script result")
	}
	return quota.Counter{Used: result[0], Allowed: result[1] == 1}, nil
}

// Refund subtracts amount and floors the counter at zero while preserving its
// expiry.
func (s *Store) Refund(ctx context.Context, key string, amount int64) (int64, error) {
	if key == "" || amount <= 0 {
		return 0, fmt.Errorf("%w: key and positive amount are required", quota.ErrInvalidRequest)
	}
	used, err := refundScript.Exec(ctx, s.client, []string{key}, []string{strconv.FormatInt(amount, 10)}).AsInt64()
	if err != nil {
		return 0, operationError("refund quota", err)
	}
	return used, nil
}

func ttlMilliseconds(ttl time.Duration) int64 {
	milliseconds := ttl.Milliseconds()
	if milliseconds == 0 && ttl > 0 {
		return 1
	}
	return milliseconds
}

func operationError(operation string, err error) error {
	if strings.Contains(strings.ToLower(err.Error()), "overflow") {
		return fmt.Errorf("%s: %w", operation, quota.ErrCounterOverflow)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
