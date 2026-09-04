# quota

`quota` is a Go library for limiting arbitrary amounts in UTC-aligned fixed
windows. The caller decides what one unit means: a request, byte, job,
recipient, model token, image, or another resource.

[![pipeline](https://github.com/acim/quota/actions/workflows/pipeline.yaml/badge.svg)](https://github.com/acim/quota/actions/workflows/pipeline.yaml)
[![reference](https://pkg.go.dev/badge/go.acim.net/quota.svg)](https://pkg.go.dev/go.acim.net/quota)

The module provides:

- atomic weighted admission without charging rejected amounts;
- atomic multi-bucket admission across independent rules and windows;
- reservations when the final amount is not known in advance;
- partial reconciliation with `Commit(actual)` and full rollback with
  `Release()`;
- isolated counters by namespace and bucket;
- an in-process memory store;
- a distributed Valkey store using a caller-owned client;
- opt-in `net/http` rate-limit middleware in `go.acim.net/quota/ratelimit`.

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

## Application key prefixes

When applications share a backing store, configure a visible key prefix for
store ACLs and operational inspection:

```go
limiter, err := quota.New(store, quota.WithKeyPrefix("myapp:quota"))
if err != nil {
	return err
}
```

The prefix changes only the store key. `Request.Namespace` still isolates
quota policies within the application. Changing the prefix of a running
deployment starts new counters; existing counters are abandoned and expire on
their own. Treat a prefix change as a quota reset and avoid it during a rolling
deployment.

## Consume multiple buckets atomically

Use `ConsumeBatch` when one operation must satisfy multiple quotas, such as a
per-minute burst limit and a per-hour sustained limit. Either every counter is
charged or none is:

```go
batch, err := limiter.ConsumeBatch(ctx, []quota.Request{
	{
		Namespace: "public-form-submissions",
		Bucket:    "ip:" + clientIP,
		Amount:    1,
		Rule:      quota.Rule{Capacity: 5, Window: time.Minute},
	},
	{
		Namespace: "public-form-submissions",
		Bucket:    "ip:" + clientIP,
		Amount:    1,
		Rule:      quota.Rule{Capacity: 30, Window: time.Hour},
	},
})
if err != nil {
	return err
}
if !batch.Allowed {
	return ErrTooManyRequests
}
```

`MemoryStore` and `ValkeyStore` implement `BatchStore`. Custom stores retain
source compatibility with `Store`, but must implement `BatchStore` before they
can be used with `ConsumeBatch`. Duplicate counters in one batch are rejected;
represent the combined amount as one request instead.

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

## HTTP middleware

The optional `ratelimit` package adapts a limiter to ordinary `net/http`
handlers. The application supplies the complete quota request, so identity,
authentication, proxy trust, and route policy remain application-owned:

```go
chatLimit, err := ratelimit.Middleware(limiter, func(req *http.Request) (quota.Request, error) {
	return quota.Request{
		Namespace: "example:http:chat",
		Bucket:    "authenticated-api",
		Amount:    1,
		Rule: quota.Rule{
			Capacity: cfg.RateLimit.Chat.Capacity,
			Window:   cfg.RateLimit.Chat.Window,
		},
	}, nil
})
if err != nil {
	return err
}

mux.Handle("POST /chats", chatLimit(http.HandlerFunc(handleChat)))
```

For endpoints governed by multiple rules, use `ratelimit.BatchMiddleware` and
return all quota requests from its mapping function. It uses `ConsumeBatch`, so
a rejected rule never charges another bucket. The middleware sets
`Retry-After` to the latest reset among the rules blocking the batch.

Allowed requests continue to the wrapped handler. Rejected requests default to
`429 Too Many Requests` with `Retry-After` set to the whole number of seconds
until the quota window resets, rounded up. Request-mapping and quota-store
errors default to a fail-closed `503 Service Unavailable` response.

Use `ratelimit.WithRejectionHandler` to customize rejected responses and
`ratelimit.WithErrorHandler` to log errors, write a custom response, or return
`true` to fail open. The middleware passes the incoming request context to the
limiter and does not select a caller identity automatically.
`ratelimit.RetryAfter` is available when the same header formatting is needed
outside middleware. A request mapper can return `ratelimit.ErrSkip` when a
configured policy is disabled or does not apply to the current request; the
middleware then continues without consuming quota or invoking the error
handler.

### Client-IP buckets

For public endpoints without an authenticated identity, the package provides
an opt-in client-IP resolver. Forwarding headers are ignored by default. When
trusted proxy CIDRs are configured, the resolver accepts `X-Forwarded-For`
only from an immediate peer inside those ranges and walks the chain from right
to left:

```go
clientIPs, err := ratelimit.NewClientIPResolver(cfg.TrustedProxyCIDRs...)
if err != nil {
	return err
}

publicChatLimit, err := ratelimit.Middleware(limiter, func(req *http.Request) (quota.Request, error) {
	ip, err := clientIPs.Resolve(req)
	if err != nil {
		return quota.Request{}, err
	}
	return quota.Request{
		Namespace: "example:http:public-chat",
		Bucket:    "ip:" + ip.String(),
		Amount:    1,
		Rule: quota.Rule{
			Capacity: cfg.RateLimit.PublicChat.Capacity,
			Window:   cfg.RateLimit.PublicChat.Window,
		},
	}, nil
})
```

With no configured proxy ranges, `Resolve` uses the normalized address from
`Request.RemoteAddr` and ignores caller-supplied forwarding headers. Proxy
ranges are deployment configuration and should match the actual ingress or
load-balancer network.

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
- Rejected batches do not change any counter in the batch.
- Store errors are returned to the caller. The application chooses whether a
  particular policy fails open or closed.
- Refunded counters floor at zero and retain their original expiry.
- Reservation idempotency is process-local. If a process terminates before a
  reservation is finalized, its admitted amount remains until the window
  resets.
- The memory store is local to one process. Use Valkey when multiple processes
  must share counters.

The root package deliberately does not include HTTP dependencies, identity
extraction, status-code policy, provider concurrency limits, or
application-specific rule names.

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
