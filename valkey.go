package quota

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

var _ Store = (*ValkeyStore)(nil)
var _ BatchStore = (*ValkeyStore)(nil)

// ValkeyStore uses a caller-owned Valkey client. Closing the store does not
// close the client.
type ValkeyStore struct {
	client valkey.Client
}

var takeScript = valkey.NewLuaScript(`
local function decimal_greater(left, right)
  if string.len(left) ~= string.len(right) then
    return string.len(left) > string.len(right)
  end
  return left > right
end

local existed = redis.call('EXISTS', KEYS[1])
local current = redis.call('GET', KEYS[1])
if not current then
  current = '0'
end
local increment = redis.pcall('INCRBY', KEYS[1], ARGV[1])
if type(increment) == 'table' and increment.err then
  return {current, 0}
end
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

var refundScript = valkey.NewLuaScript(`
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

var takeBatchScript = valkey.NewLuaScript(`
local function decimal_greater(left, right)
  if string.len(left) ~= string.len(right) then
    return string.len(left) > string.len(right)
  end
  return left > right
end

local function rollback(last, existed, amounts)
  for index = 1, last do
    if existed[index] == 0 then
      redis.call('DEL', KEYS[index])
    else
      redis.call('DECRBY', KEYS[index], amounts[index])
    end
  end
end

local previous = {}
local existed = {}
local amounts = {}
for index = 1, #KEYS do
  existed[index] = redis.call('EXISTS', KEYS[index])
  previous[index] = redis.call('GET', KEYS[index]) or '0'
  amounts[index] = ARGV[(index - 1) * 3 + 1]
end

for index = 1, #KEYS do
  local offset = (index - 1) * 3
  local increment = redis.pcall('INCRBY', KEYS[index], amounts[index])
  if type(increment) == 'table' and increment.err then
    rollback(index - 1, existed, amounts)
    return redis.error_reply('quota counter overflow')
  end
  local current = redis.call('GET', KEYS[index])
  if decimal_greater(current, ARGV[offset + 2]) then
    rollback(index, existed, amounts)
    local rejected = {0}
    for resultIndex = 1, #KEYS do
      rejected[#rejected + 1] = previous[resultIndex]
    end
    return rejected
  end
  if existed[index] == 0 then
    redis.call('PEXPIRE', KEYS[index], ARGV[offset + 3])
  end
end

local accepted = {1}
for index = 1, #KEYS do
  accepted[#accepted + 1] = redis.call('GET', KEYS[index])
end
return accepted
`)

// NewValkeyStore creates a store using client. The caller retains ownership of
// the client and remains responsible for its configuration and lifecycle.
func NewValkeyStore(client valkey.Client) (*ValkeyStore, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: valkey client is required", ErrInvalidRequest)
	}
	return &ValkeyStore{client: client}, nil
}

// Take atomically admits amount when it fits within capacity.
func (s *ValkeyStore) Take(ctx context.Context, key string, amount, capacity int64, ttl time.Duration) (Counter, error) {
	if key == "" || amount <= 0 || capacity <= 0 || ttl <= 0 {
		return Counter{}, fmt.Errorf("%w: key, amount, capacity, and ttl must be positive", ErrInvalidRequest)
	}
	result, err := takeScript.Exec(
		ctx,
		s.client,
		[]string{key},
		[]string{strconv.FormatInt(amount, 10), strconv.FormatInt(capacity, 10), strconv.FormatInt(ttlMilliseconds(ttl), 10)},
	).AsIntSlice()
	if err != nil {
		return Counter{}, valkeyOperationError("take quota", err)
	}
	if len(result) != 2 {
		return Counter{}, errors.New("take quota: invalid script result")
	}
	return Counter{Used: result[0], Allowed: result[1] == 1}, nil
}

// TakeBatch atomically admits every take or leaves every counter unchanged.
func (s *ValkeyStore) TakeBatch(ctx context.Context, takes []BatchTake) (BatchCounter, error) {
	if err := validateBatchTakes(takes); err != nil {
		return BatchCounter{}, err
	}
	keys := make([]string, len(takes))
	arguments := make([]string, 0, len(takes)*3)
	for index, take := range takes {
		keys[index] = take.Key
		arguments = append(arguments,
			strconv.FormatInt(take.Amount, 10),
			strconv.FormatInt(take.Capacity, 10),
			strconv.FormatInt(ttlMilliseconds(take.TTL), 10),
		)
	}
	result, err := takeBatchScript.Exec(ctx, s.client, keys, arguments).AsIntSlice()
	if err != nil {
		return BatchCounter{}, valkeyOperationError("take quota batch", err)
	}
	if len(result) != len(takes)+1 {
		return BatchCounter{}, errors.New("take quota batch: invalid script result")
	}
	return BatchCounter{Allowed: result[0] == 1, Used: result[1:]}, nil
}

// Refund subtracts amount and floors the counter at zero while preserving its
// expiry.
func (s *ValkeyStore) Refund(ctx context.Context, key string, amount int64) (int64, error) {
	if key == "" || amount <= 0 {
		return 0, fmt.Errorf("%w: key and positive amount are required", ErrInvalidRequest)
	}
	used, err := refundScript.Exec(ctx, s.client, []string{key}, []string{strconv.FormatInt(amount, 10)}).AsInt64()
	if err != nil {
		return 0, valkeyOperationError("refund quota", err)
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

func valkeyOperationError(operation string, err error) error {
	if strings.Contains(strings.ToLower(err.Error()), "overflow") {
		return fmt.Errorf("%s: %w", operation, ErrCounterOverflow)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
