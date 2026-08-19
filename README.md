# quota

`quota` is a Go library for limiting arbitrary amounts in UTC-aligned fixed
windows. The caller decides what one unit means: a request, byte, job,
recipient, model token, image, or another resource.

The module provides:

- atomic weighted admission without charging rejected amounts;
- reservations when the final amount is not known in advance;
- partial reconciliation with `Commit(actual)` and full rollback with
  `Release()`;
- isolated counters by namespace and bucket;
- an in-process memory store;
- a distributed Valkey store using a caller-owned client.

## Install

```sh
go get go.acim.net/quota
```

## Consume a known amount

```go
store := quota.NewMemoryStore()
limiter, err := quota.New(store)
if err != nil {
	return err
}

decision, err := limiter.Consume(ctx, quota.Request{
	Namespace: "public-chat-requests",
	Bucket:    sessionID,
	Amount:    1,
	Rule: quota.Rule{
		Capacity: 20,
		Window:   time.Minute,
	},
})
if err != nil {
	return err
}
if !decision.Allowed {
	return ErrTooManyRequests
}
```

## Reserve an upper bound

Reserve the maximum amount before starting expensive work, then reconcile it
with the amount actually used:

```go
decision, reservation, err := limiter.Reserve(ctx, quota.Request{
	Namespace: "daily-compute",
	Bucket:    accountID,
	Amount:    maximumCost,
	Rule: quota.Rule{
		Capacity: 50_000,
		Window:   24 * time.Hour,
	},
})
if err != nil {
	return err
}
if !decision.Allowed {
	return ErrBudgetExhausted
}

actualCost, err := performWork(ctx)
if err != nil {
	_ = reservation.Release(context.WithoutCancel(ctx))
	return err
}
if err := reservation.Commit(ctx, actualCost); err != nil {
	return err
}
```

`actualCost` must be between zero and the reserved amount. Reservations are
idempotent within the current process when finalized repeatedly with the same
result.

## Valkey

The Valkey store does not own the client, so applications retain control over
connection configuration, TLS, lifecycle, and observability:

```go
client, err := valkey.NewClient(options)
if err != nil {
	return err
}
defer client.Close()

store, err := quota.NewValkeyStore(client)
if err != nil {
	return err
}
limiter, err := quota.New(store)
```

## Semantics

- Windows are fixed and aligned to UTC epoch boundaries. A 24-hour window
  resets at 00:00 UTC.
- Rejected amounts do not change the counter.
- Store errors are returned to the caller. The application chooses whether a
  particular policy fails open or closed.
- Refunded counters floor at zero and retain their original expiry.
- Reservation idempotency is process-local. If a process terminates before a
  reservation is finalized, its admitted amount remains until the window
  resets.
- The memory store is local to one process. Use Valkey when multiple processes
  must share counters.

The library deliberately does not include HTTP middleware, identity extraction,
status-code policy, provider concurrency limits, or application-specific rule
names.

## Development

The integration suite runs against Valkey from `compose.yaml`:

```sh
make integration-up
make integration-test
make coverage
make integration-down
```

The Compose service binds Valkey only to `127.0.0.1:16379`. Set
`QUOTA_VALKEY_URL` to use another test instance.

## License

Licensed under either of

- Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license
  ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
