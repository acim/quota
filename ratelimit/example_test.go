package ratelimit_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"go.acim.net/quota"
	"go.acim.net/quota/ratelimit"
)

func ExampleMiddleware() {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	limiter, err := quota.New(
		quota.NewMemoryStore(),
		quota.WithClock(func() time.Time { return now }),
	)
	if err != nil {
		panic(err)
	}

	limit, err := ratelimit.Middleware(limiter, func(*http.Request) (quota.Request, error) {
		return quota.Request{
			Namespace: "example:http:chat",
			Bucket:    "authenticated-api",
			Amount:    1,
			Rule:      quota.Rule{Capacity: 1, Window: time.Minute},
		}, nil
	}, ratelimit.WithClock(func() time.Time { return now }))
	if err != nil {
		panic(err)
	}

	handler := limit(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	for range 2 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/chats", nil))
		fmt.Print(response.Code)
		if retryAfter := response.Header().Get("Retry-After"); retryAfter != "" {
			fmt.Print(" ", retryAfter)
		}
		fmt.Println()
	}

	// Output:
	// 204
	// 429 60
}

func ExampleBatchMiddleware() {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	limiter, err := quota.New(
		quota.NewMemoryStore(),
		quota.WithClock(func() time.Time { return now }),
	)
	if err != nil {
		panic(err)
	}

	limit, err := ratelimit.BatchMiddleware(limiter, func(*http.Request) ([]quota.Request, error) {
		return []quota.Request{
			{Namespace: "example:http:forms", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 2, Window: time.Minute}},
			{Namespace: "example:http:forms", Bucket: "client", Amount: 1, Rule: quota.Rule{Capacity: 1, Window: time.Hour}},
		}, nil
	}, ratelimit.WithClock(func() time.Time { return now }))
	if err != nil {
		panic(err)
	}

	handler := limit(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	for range 2 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/forms", nil))
		fmt.Print(response.Code)
		if retryAfter := response.Header().Get("Retry-After"); retryAfter != "" {
			fmt.Print(" ", retryAfter)
		}
		fmt.Println()
	}

	// Output:
	// 204
	// 429 3600
}

func ExampleClientIPResolver() {
	resolver, err := ratelimit.NewClientIPResolver("10.0.0.0/8")
	if err != nil {
		panic(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/public/chats", nil)
	request.RemoteAddr = "10.0.0.3:4321"
	request.Header.Set("X-Forwarded-For", "198.51.100.20, 10.0.0.2")
	ip, err := resolver.Resolve(request)
	if err != nil {
		panic(err)
	}

	fmt.Println(ip)
	// Output: 198.51.100.20
}
