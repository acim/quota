// Package ratelimit provides net/http middleware backed by a quota consumer.
//
// Applications retain ownership of identity, proxy trust, authentication, and
// quota policy by mapping each HTTP request to a complete quota.Request.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"go.acim.net/quota"
)

// ErrSkip tells middleware that the current request is not subject to its
// policy. The downstream handler continues without calling the consumer or the
// configured error handler.
var ErrSkip = errors.New("skip rate limit")

// Consumer admits quota requests. *quota.Limiter satisfies Consumer.
type Consumer interface {
	Consume(context.Context, quota.Request) (quota.Decision, error)
}

var _ Consumer = (*quota.Limiter)(nil)

// BatchConsumer atomically admits multiple quota requests. *quota.Limiter
// satisfies BatchConsumer when its store implements quota.BatchStore.
type BatchConsumer interface {
	ConsumeBatch(context.Context, []quota.Request) (quota.BatchDecision, error)
}

var _ BatchConsumer = (*quota.Limiter)(nil)

// RequestFunc maps an HTTP request to the complete quota request that should
// be consumed. Applications use it to choose identity and quota policy.
type RequestFunc func(*http.Request) (quota.Request, error)

// BatchRequestFunc maps an HTTP request to quota requests that must be admitted
// atomically.
type BatchRequestFunc func(*http.Request) ([]quota.Request, error)

// RejectionHandler writes the response for a rejected quota decision.
type RejectionHandler func(http.ResponseWriter, *http.Request, quota.Decision)

// ErrorHandler handles a request-mapping or consumer error. Returning true
// continues to the downstream handler (fail open). Returning false stops the
// request after the callback returns (fail closed). A handler that returns true
// should not write a response.
type ErrorHandler func(http.ResponseWriter, *http.Request, error) (continueRequest bool)

// Option configures HTTP rate-limit middleware.
type Option interface {
	apply(*config) error
}

type optionFunc func(*config) error

func (option optionFunc) apply(config *config) error {
	return option(config)
}

type config struct {
	now              func() time.Time
	rejectionHandler RejectionHandler
	errorHandler     ErrorHandler
}

// WithClock configures the clock used to calculate Retry-After.
func WithClock(now func() time.Time) Option {
	return optionFunc(func(config *config) error {
		if now == nil {
			return errors.New("clock is required")
		}
		config.now = now
		return nil
	})
}

// WithRejectionHandler replaces the default 429 response handler.
// Retry-After is set before the handler is called.
func WithRejectionHandler(handler RejectionHandler) Option {
	return optionFunc(func(config *config) error {
		if handler == nil {
			return errors.New("rejection handler is required")
		}
		config.rejectionHandler = handler
		return nil
	})
}

// WithErrorHandler replaces the default fail-closed 503 response handler.
// Return true from handler to fail open and continue to the downstream handler.
func WithErrorHandler(handler ErrorHandler) Option {
	return optionFunc(func(config *config) error {
		if handler == nil {
			return errors.New("error handler is required")
		}
		config.errorHandler = handler
		return nil
	})
}

// Middleware constructs net/http middleware backed by consumer.
func Middleware(consumer Consumer, mapRequest RequestFunc, options ...Option) (func(http.Handler) http.Handler, error) {
	if isNilValue(consumer) {
		return nil, errors.New("consumer is required")
	}
	if mapRequest == nil {
		return nil, errors.New("request function is required")
	}

	config := config{
		now:              time.Now,
		rejectionHandler: defaultRejectionHandler,
		errorHandler:     defaultErrorHandler,
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("option is required")
		}
		if err := option.apply(&config); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			quotaRequest, err := mapRequest(request)
			if err != nil {
				if errors.Is(err, ErrSkip) {
					next.ServeHTTP(writer, request)
					return
				}
				if config.errorHandler(writer, request, fmt.Errorf("map quota request: %w", err)) {
					next.ServeHTTP(writer, request)
				}
				return
			}

			decision, err := consumer.Consume(request.Context(), quotaRequest)
			if err != nil {
				if config.errorHandler(writer, request, fmt.Errorf("consume quota: %w", err)) {
					next.ServeHTTP(writer, request)
				}
				return
			}
			if decision.Allowed {
				next.ServeHTTP(writer, request)
				return
			}

			writer.Header().Set("Retry-After", RetryAfter(decision.ResetAt, config.now()))
			config.rejectionHandler(writer, request, decision)
		})
	}, nil
}

// BatchMiddleware constructs net/http middleware that atomically admits every
// quota request returned by mapRequest.
func BatchMiddleware(consumer BatchConsumer, mapRequest BatchRequestFunc, options ...Option) (func(http.Handler) http.Handler, error) {
	if isNilValue(consumer) {
		return nil, errors.New("batch consumer is required")
	}
	if mapRequest == nil {
		return nil, errors.New("batch request function is required")
	}

	config := config{
		now:              time.Now,
		rejectionHandler: defaultRejectionHandler,
		errorHandler:     defaultErrorHandler,
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("option is required")
		}
		if err := option.apply(&config); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			quotaRequests, err := mapRequest(request)
			if err != nil {
				if errors.Is(err, ErrSkip) {
					next.ServeHTTP(writer, request)
					return
				}
				if config.errorHandler(writer, request, fmt.Errorf("map quota batch request: %w", err)) {
					next.ServeHTTP(writer, request)
				}
				return
			}

			decision, err := consumer.ConsumeBatch(request.Context(), quotaRequests)
			if err != nil {
				if config.errorHandler(writer, request, fmt.Errorf("consume quota batch: %w", err)) {
					next.ServeHTTP(writer, request)
				}
				return
			}
			if decision.Allowed {
				next.ServeHTTP(writer, request)
				return
			}

			blocking := blockingDecision(decision)
			writer.Header().Set("Retry-After", RetryAfter(decision.RetryAt(), config.now()))
			config.rejectionHandler(writer, request, blocking)
		})
	}, nil
}

func blockingDecision(batch quota.BatchDecision) quota.Decision {
	retryAt := batch.RetryAt()
	for _, decision := range batch.Decisions {
		if !decision.Allowed && decision.ResetAt.Equal(retryAt) {
			return decision
		}
	}
	return quota.Decision{Allowed: false, ResetAt: retryAt}
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// RetryAfter returns the whole number of seconds until resetAt, rounded up for
// use in an HTTP Retry-After header.
func RetryAfter(resetAt, now time.Time) string {
	return RetryAfterDuration(resetAt.Sub(now))
}

// RetryAfterDuration returns a Retry-After value for a known remaining
// duration.
func RetryAfterDuration(remaining time.Duration) string {
	if remaining <= 0 {
		return "0"
	}
	seconds := remaining / time.Second
	if remaining%time.Second != 0 {
		seconds++
	}
	return strconv.FormatInt(int64(seconds), 10)
}

func defaultRejectionHandler(writer http.ResponseWriter, _ *http.Request, _ quota.Decision) {
	http.Error(writer, "rate limit exceeded", http.StatusTooManyRequests)
}

func defaultErrorHandler(writer http.ResponseWriter, _ *http.Request, _ error) bool {
	http.Error(writer, "rate limit unavailable", http.StatusServiceUnavailable)
	return false
}
