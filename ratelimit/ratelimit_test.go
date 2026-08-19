package ratelimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"go.acim.net/quota"
)

func TestMiddlewareRejectsInvalidConstructionArguments(t *testing.T) {
	t.Parallel()
	validConsumer := consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
		return quota.Decision{Allowed: true}, nil
	})
	validRequest := func(*http.Request) (quota.Request, error) { return quota.Request{}, nil }

	tests := []struct {
		name     string
		consumer Consumer
		request  RequestFunc
		options  []Option
	}{
		{name: "nil consumer", request: validRequest},
		{name: "typed nil consumer", consumer: (*nilConsumer)(nil), request: validRequest},
		{name: "nil request function", consumer: validConsumer},
		{name: "nil option", consumer: validConsumer, request: validRequest, options: []Option{nil}},
		{name: "nil clock", consumer: validConsumer, request: validRequest, options: []Option{WithClock(nil)}},
		{name: "nil rejection handler", consumer: validConsumer, request: validRequest, options: []Option{WithRejectionHandler(nil)}},
		{name: "nil error handler", consumer: validConsumer, request: validRequest, options: []Option{WithErrorHandler(nil)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Middleware(test.consumer, test.request, test.options...); err == nil {
				t.Fatal("Middleware() error = nil, want non-nil")
			}
		})
	}
}

func TestMiddlewareAllowsRequestAndForwardsContextAndMappedRequest(t *testing.T) {
	t.Parallel()
	type contextKey string
	const key contextKey = "request-id"
	wantRequest := quota.Request{
		Namespace: "example:http:chat",
		Bucket:    "client-42",
		Amount:    2,
		Rule:      quota.Rule{Capacity: 10, Window: time.Minute},
	}
	var gotContext context.Context
	var gotRequest quota.Request
	consumer := consumerFunc(func(ctx context.Context, request quota.Request) (quota.Decision, error) {
		gotContext = ctx
		gotRequest = request
		return quota.Decision{Allowed: true}, nil
	})
	middleware, err := Middleware(consumer, func(*http.Request) (quota.Request, error) {
		return wantRequest, nil
	})
	if err != nil {
		t.Fatalf("Middleware() error = %v", err)
	}

	called := 0
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called++
	}))
	request := httptest.NewRequest(http.MethodPost, "/chats", nil)
	request = request.WithContext(context.WithValue(request.Context(), key, "abc123"))
	handler.ServeHTTP(httptest.NewRecorder(), request)

	if called != 1 {
		t.Fatalf("downstream calls = %d, want 1", called)
	}
	if gotContext != request.Context() || gotContext.Value(key) != "abc123" {
		t.Fatal("Consume() did not receive the incoming request context")
	}
	if !reflect.DeepEqual(gotRequest, wantRequest) {
		t.Fatalf("Consume() request = %+v, want %+v", gotRequest, wantRequest)
	}
}

func TestMiddlewareRejectsRequestWithRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 19, 12, 0, 0, 250_000_000, time.UTC)
	tests := []struct {
		name    string
		resetAt time.Time
		want    string
	}{
		{name: "fractional second rounds up", resetAt: now.Add(1501 * time.Millisecond), want: "2"},
		{name: "whole second", resetAt: now.Add(2 * time.Second), want: "2"},
		{name: "expired reset floors at zero", resetAt: now.Add(-time.Second), want: "0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			consumer := consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
				return quota.Decision{Allowed: false, ResetAt: test.resetAt}, nil
			})
			middleware, err := Middleware(consumer, staticRequest, WithClock(func() time.Time { return now }))
			if err != nil {
				t.Fatalf("Middleware() error = %v", err)
			}
			called := 0
			recorder := httptest.NewRecorder()
			middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(
				recorder,
				httptest.NewRequest(http.MethodGet, "/", nil),
			)
			if called != 0 {
				t.Fatalf("downstream calls = %d, want 0", called)
			}
			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
			}
			if got := recorder.Header().Get("Retry-After"); got != test.want {
				t.Fatalf("Retry-After = %q, want %q", got, test.want)
			}
		})
	}
}

func TestMiddlewareDefaultsToFailClosedForMappingAndConsumerErrors(t *testing.T) {
	t.Parallel()
	mapErr := errors.New("map request")
	storeErr := errors.New("store unavailable")
	tests := []struct {
		name     string
		consumer Consumer
		request  RequestFunc
	}{
		{
			name: "mapping error",
			consumer: consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
				t.Fatal("Consume() called after mapping error")
				return quota.Decision{}, nil
			}),
			request: func(*http.Request) (quota.Request, error) { return quota.Request{}, mapErr },
		},
		{
			name: "consumer error",
			consumer: consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
				return quota.Decision{}, storeErr
			}),
			request: staticRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			middleware, err := Middleware(test.consumer, test.request)
			if err != nil {
				t.Fatalf("Middleware() error = %v", err)
			}
			called := 0
			recorder := httptest.NewRecorder()
			middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(
				recorder,
				httptest.NewRequest(http.MethodGet, "/", nil),
			)
			if called != 0 {
				t.Fatalf("downstream calls = %d, want 0", called)
			}
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestMiddlewareSkipBypassesAdmissionAndErrorHandling(t *testing.T) {
	t.Parallel()
	consumer := consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
		t.Fatal("Consume() called for skipped request")
		return quota.Decision{}, nil
	})
	errorCalls := 0
	middleware, err := Middleware(consumer, func(*http.Request) (quota.Request, error) {
		return quota.Request{}, ErrSkip
	}, WithErrorHandler(func(http.ResponseWriter, *http.Request, error) bool {
		errorCalls++
		return false
	}))
	if err != nil {
		t.Fatalf("Middleware() error = %v", err)
	}
	called := 0
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if called != 1 || errorCalls != 0 {
		t.Fatalf("downstream calls = %d, error calls = %d; want 1, 0", called, errorCalls)
	}
}

func TestCustomErrorHandlerCanFailOpenExactlyOnce(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("store unavailable")
	consumer := consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
		return quota.Decision{}, wantErr
	})
	var gotRequest *http.Request
	var gotErr error
	middleware, err := Middleware(consumer, staticRequest, WithErrorHandler(func(_ http.ResponseWriter, request *http.Request, err error) bool {
		gotRequest = request
		gotErr = err
		return true
	}))
	if err != nil {
		t.Fatalf("Middleware() error = %v", err)
	}

	called := 0
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(httptest.NewRecorder(), request)
	if called != 1 {
		t.Fatalf("downstream calls = %d, want 1", called)
	}
	if gotRequest != request || !errors.Is(gotErr, wantErr) {
		t.Fatalf("error handler received request %p and error %v", gotRequest, gotErr)
	}
}

func TestCustomHandlersReceiveRequestAndOutcome(t *testing.T) {
	t.Parallel()
	t.Run("rejection", func(t *testing.T) {
		t.Parallel()
		wantDecision := quota.Decision{Allowed: false, Capacity: 5, Used: 5}
		consumer := consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
			return wantDecision, nil
		})
		var gotRequest *http.Request
		var gotDecision quota.Decision
		middleware, err := Middleware(consumer, staticRequest, WithRejectionHandler(func(w http.ResponseWriter, request *http.Request, decision quota.Decision) {
			gotRequest = request
			gotDecision = decision
			w.WriteHeader(http.StatusTeapot)
		}))
		if err != nil {
			t.Fatalf("Middleware() error = %v", err)
		}
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		recorder := httptest.NewRecorder()
		middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("downstream handler called")
		})).ServeHTTP(recorder, request)
		if recorder.Code != http.StatusTeapot || gotRequest != request || !reflect.DeepEqual(gotDecision, wantDecision) {
			t.Fatalf("custom rejection response = %d, request = %p, decision = %+v", recorder.Code, gotRequest, gotDecision)
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("map request")
		var gotRequest *http.Request
		var gotErr error
		middleware, err := Middleware(consumerFunc(func(context.Context, quota.Request) (quota.Decision, error) {
			t.Fatal("Consume() called after mapping error")
			return quota.Decision{}, nil
		}), func(*http.Request) (quota.Request, error) {
			return quota.Request{}, wantErr
		}, WithErrorHandler(func(w http.ResponseWriter, request *http.Request, err error) bool {
			gotRequest = request
			gotErr = err
			w.WriteHeader(http.StatusBadRequest)
			return false
		}))
		if err != nil {
			t.Fatalf("Middleware() error = %v", err)
		}
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		recorder := httptest.NewRecorder()
		middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("downstream handler called")
		})).ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || gotRequest != request || !errors.Is(gotErr, wantErr) {
			t.Fatalf("custom error response = %d, request = %p, error = %v", recorder.Code, gotRequest, gotErr)
		}
	})
}

func TestMiddlewarePropagatesCanceledContext(t *testing.T) {
	t.Parallel()
	consumer := consumerFunc(func(ctx context.Context, _ quota.Request) (quota.Decision, error) {
		return quota.Decision{}, ctx.Err()
	})
	var gotErr error
	middleware, err := Middleware(consumer, staticRequest, WithErrorHandler(func(_ http.ResponseWriter, _ *http.Request, err error) bool {
		gotErr = err
		return false
	}))
	if err != nil {
		t.Fatalf("Middleware() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("downstream handler called")
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("error handler error = %v, want context.Canceled", gotErr)
	}
}

func TestMiddlewareWithMemoryStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	limiter, err := quota.New(quota.NewMemoryStore(), quota.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("quota.New() error = %v", err)
	}
	middleware, err := Middleware(limiter, func(*http.Request) (quota.Request, error) {
		return quota.Request{
			Namespace: "example:http:chat",
			Bucket:    "authenticated-api",
			Amount:    1,
			Rule:      quota.Rule{Capacity: 2, Window: time.Minute},
		}, nil
	}, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("Middleware() error = %v", err)
	}
	called := 0
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))

	for requestNumber, wantStatus := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/chats", nil))
		if recorder.Code != wantStatus {
			t.Fatalf("request %d status = %d, want %d", requestNumber+1, recorder.Code, wantStatus)
		}
	}
	if called != 2 {
		t.Fatalf("downstream calls = %d, want 2", called)
	}
}

func TestBatchMiddlewareRejectsAtomicallyWithLatestBlockingRetry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	wantRequests := []quota.Request{
		{Namespace: "forms", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 5, Window: time.Minute}},
		{Namespace: "forms", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 30, Window: time.Hour}},
	}
	consumer := batchConsumerFunc(func(_ context.Context, requests []quota.Request) (quota.BatchDecision, error) {
		if !reflect.DeepEqual(requests, wantRequests) {
			t.Fatalf("ConsumeBatch() requests = %+v, want %+v", requests, wantRequests)
		}
		return quota.BatchDecision{Allowed: false, Decisions: []quota.Decision{
			{Allowed: false, Requested: 1, Capacity: 5, Used: 5, ResetAt: now.Add(time.Minute)},
			{Allowed: false, Requested: 1, Capacity: 30, Used: 30, ResetAt: now.Add(time.Hour)},
		}}, nil
	})
	middleware, err := BatchMiddleware(consumer, func(*http.Request) ([]quota.Request, error) {
		return wantRequests, nil
	}, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("BatchMiddleware() error = %v", err)
	}

	called := 0
	recorder := httptest.NewRecorder()
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPost, "/forms/123/submissions", nil),
	)
	if called != 0 {
		t.Fatalf("downstream calls = %d, want 0", called)
	}
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if got := recorder.Header().Get("Retry-After"); got != "3600" {
		t.Fatalf("Retry-After = %q, want %q", got, "3600")
	}
}

func TestBatchMiddlewareAllowsRequestAndForwardsContextAndMappedRequests(t *testing.T) {
	t.Parallel()
	type contextKey string
	const key contextKey = "request-id"
	wantRequests := []quota.Request{
		{Namespace: "example:http:forms", Bucket: "client-42", Amount: 1, Rule: quota.Rule{Capacity: 5, Window: time.Minute}},
		{Namespace: "example:http:forms", Bucket: "client-42", Amount: 1, Rule: quota.Rule{Capacity: 30, Window: time.Hour}},
	}
	var gotContext context.Context
	var gotRequests []quota.Request
	consumer := batchConsumerFunc(func(ctx context.Context, requests []quota.Request) (quota.BatchDecision, error) {
		gotContext = ctx
		gotRequests = requests
		return quota.BatchDecision{Allowed: true}, nil
	})
	middleware, err := BatchMiddleware(consumer, func(*http.Request) ([]quota.Request, error) {
		return wantRequests, nil
	})
	if err != nil {
		t.Fatalf("BatchMiddleware() error = %v", err)
	}

	called := 0
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))
	request := httptest.NewRequest(http.MethodPost, "/forms", nil)
	request = request.WithContext(context.WithValue(request.Context(), key, "abc123"))
	handler.ServeHTTP(httptest.NewRecorder(), request)

	if called != 1 {
		t.Fatalf("downstream calls = %d, want 1", called)
	}
	if gotContext != request.Context() || gotContext.Value(key) != "abc123" {
		t.Fatal("ConsumeBatch() did not receive the incoming request context")
	}
	if !reflect.DeepEqual(gotRequests, wantRequests) {
		t.Fatalf("ConsumeBatch() requests = %+v, want %+v", gotRequests, wantRequests)
	}
}

func TestBatchMiddlewareDefaultsToFailClosedForMappingAndConsumerErrors(t *testing.T) {
	t.Parallel()
	mapErr := errors.New("map batch request")
	storeErr := errors.New("store unavailable")
	tests := []struct {
		name     string
		consumer BatchConsumer
		request  BatchRequestFunc
	}{
		{
			name: "mapping error",
			consumer: batchConsumerFunc(func(context.Context, []quota.Request) (quota.BatchDecision, error) {
				t.Fatal("ConsumeBatch() called after mapping error")
				return quota.BatchDecision{}, nil
			}),
			request: func(*http.Request) ([]quota.Request, error) { return nil, mapErr },
		},
		{
			name: "consumer error",
			consumer: batchConsumerFunc(func(context.Context, []quota.Request) (quota.BatchDecision, error) {
				return quota.BatchDecision{}, storeErr
			}),
			request: staticBatchRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			middleware, err := BatchMiddleware(test.consumer, test.request)
			if err != nil {
				t.Fatalf("BatchMiddleware() error = %v", err)
			}
			called := 0
			recorder := httptest.NewRecorder()
			middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(
				recorder,
				httptest.NewRequest(http.MethodGet, "/", nil),
			)
			if called != 0 {
				t.Fatalf("downstream calls = %d, want 0", called)
			}
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestBatchMiddlewareSkipBypassesAdmissionAndErrorHandling(t *testing.T) {
	t.Parallel()
	consumer := batchConsumerFunc(func(context.Context, []quota.Request) (quota.BatchDecision, error) {
		t.Fatal("ConsumeBatch() called for skipped request")
		return quota.BatchDecision{}, nil
	})
	errorCalls := 0
	middleware, err := BatchMiddleware(consumer, func(*http.Request) ([]quota.Request, error) {
		return nil, ErrSkip
	}, WithErrorHandler(func(http.ResponseWriter, *http.Request, error) bool {
		errorCalls++
		return false
	}))
	if err != nil {
		t.Fatalf("BatchMiddleware() error = %v", err)
	}
	called := 0
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if called != 1 || errorCalls != 0 {
		t.Fatalf("downstream calls = %d, error calls = %d; want 1, 0", called, errorCalls)
	}
}

func TestBatchMiddlewareCustomErrorHandlerCanFailOpenExactlyOnce(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("store unavailable")
	consumer := batchConsumerFunc(func(context.Context, []quota.Request) (quota.BatchDecision, error) {
		return quota.BatchDecision{}, wantErr
	})
	var gotRequest *http.Request
	var gotErr error
	middleware, err := BatchMiddleware(consumer, staticBatchRequest, WithErrorHandler(func(_ http.ResponseWriter, request *http.Request, err error) bool {
		gotRequest = request
		gotErr = err
		return true
	}))
	if err != nil {
		t.Fatalf("BatchMiddleware() error = %v", err)
	}

	called := 0
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(httptest.NewRecorder(), request)
	if called != 1 {
		t.Fatalf("downstream calls = %d, want 1", called)
	}
	if gotRequest != request || !errors.Is(gotErr, wantErr) {
		t.Fatalf("error handler received request %p and error %v", gotRequest, gotErr)
	}
}

func TestBatchMiddlewareCustomHandlersReceiveRequestAndOutcome(t *testing.T) {
	t.Parallel()
	t.Run("rejection", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
		wantDecision := quota.Decision{Allowed: false, Capacity: 30, Used: 30, ResetAt: now.Add(time.Hour)}
		consumer := batchConsumerFunc(func(context.Context, []quota.Request) (quota.BatchDecision, error) {
			return quota.BatchDecision{Allowed: false, Decisions: []quota.Decision{
				{Allowed: false, Capacity: 5, Used: 5, ResetAt: now.Add(time.Minute)},
				wantDecision,
			}}, nil
		})
		var gotRequest *http.Request
		var gotDecision quota.Decision
		middleware, err := BatchMiddleware(consumer, staticBatchRequest, WithRejectionHandler(func(w http.ResponseWriter, request *http.Request, decision quota.Decision) {
			gotRequest = request
			gotDecision = decision
			w.WriteHeader(http.StatusTeapot)
		}))
		if err != nil {
			t.Fatalf("BatchMiddleware() error = %v", err)
		}
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		recorder := httptest.NewRecorder()
		middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("downstream handler called")
		})).ServeHTTP(recorder, request)
		if recorder.Code != http.StatusTeapot || gotRequest != request || !reflect.DeepEqual(gotDecision, wantDecision) {
			t.Fatalf("custom rejection response = %d, request = %p, decision = %+v", recorder.Code, gotRequest, gotDecision)
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("map batch request")
		var gotRequest *http.Request
		var gotErr error
		middleware, err := BatchMiddleware(batchConsumerFunc(func(context.Context, []quota.Request) (quota.BatchDecision, error) {
			t.Fatal("ConsumeBatch() called after mapping error")
			return quota.BatchDecision{}, nil
		}), func(*http.Request) ([]quota.Request, error) {
			return nil, wantErr
		}, WithErrorHandler(func(w http.ResponseWriter, request *http.Request, err error) bool {
			gotRequest = request
			gotErr = err
			w.WriteHeader(http.StatusBadRequest)
			return false
		}))
		if err != nil {
			t.Fatalf("BatchMiddleware() error = %v", err)
		}
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		recorder := httptest.NewRecorder()
		middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("downstream handler called")
		})).ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || gotRequest != request || !errors.Is(gotErr, wantErr) {
			t.Fatalf("custom error response = %d, request = %p, error = %v", recorder.Code, gotRequest, gotErr)
		}
	})
}

func TestBatchMiddlewareWithMemoryStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	limiter, err := quota.New(quota.NewMemoryStore(), quota.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("quota.New() error = %v", err)
	}
	requests := []quota.Request{
		{Namespace: "forms", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 2, Window: time.Minute}},
		{Namespace: "forms", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 1, Window: time.Hour}},
	}
	middleware, err := BatchMiddleware(limiter, func(*http.Request) ([]quota.Request, error) {
		return requests, nil
	}, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("BatchMiddleware() error = %v", err)
	}
	called := 0
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))

	for requestNumber, wantStatus := range []int{http.StatusOK, http.StatusTooManyRequests} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/forms/123/submissions", nil))
		if recorder.Code != wantStatus {
			t.Fatalf("request %d status = %d, want %d", requestNumber+1, recorder.Code, wantStatus)
		}
	}
	if called != 1 {
		t.Fatalf("downstream calls = %d, want 1", called)
	}

	minute, err := limiter.Consume(context.Background(), requests[0])
	if err != nil || !minute.Allowed || minute.Used != 2 {
		t.Fatalf("minute Consume() after rejected batch = %+v, %v; want atomic rollback", minute, err)
	}
}

type consumerFunc func(context.Context, quota.Request) (quota.Decision, error)

func (consume consumerFunc) Consume(ctx context.Context, request quota.Request) (quota.Decision, error) {
	return consume(ctx, request)
}

type batchConsumerFunc func(context.Context, []quota.Request) (quota.BatchDecision, error)

func (consume batchConsumerFunc) ConsumeBatch(ctx context.Context, requests []quota.Request) (quota.BatchDecision, error) {
	return consume(ctx, requests)
}

type nilConsumer struct{}

func (*nilConsumer) Consume(context.Context, quota.Request) (quota.Decision, error) {
	panic("typed nil consumer should be rejected during construction")
}

func staticRequest(*http.Request) (quota.Request, error) {
	return quota.Request{Namespace: "api", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 1, Window: time.Minute}}, nil
}

func staticBatchRequest(*http.Request) ([]quota.Request, error) {
	return []quota.Request{{Namespace: "api", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 1, Window: time.Minute}}}, nil
}
