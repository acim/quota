.PHONY: lint test integration-up integration-down integration-test coverage check update

COVERAGE_THRESHOLD := 95.0
QUOTA_VALKEY_URL ?= redis://localhost:16379/0

lint:
	@golangci-lint run

test:
	@go test -race -v ./...

integration-up:
	@podman compose up -d --wait

integration-down:
	@podman compose down

integration-test:
	@QUOTA_VALKEY_URL=$(QUOTA_VALKEY_URL) go test -race -count=1 -v .

coverage:
	@QUOTA_VALKEY_URL=$(QUOTA_VALKEY_URL) go test -race -count=1 -coverprofile=coverage.out .
	@coverage=$$(go tool cover -func=coverage.out | awk '/^total:/ { gsub("%", "", $$3); print $$3 }'); \
	awk -v coverage="$$coverage" -v threshold="$(COVERAGE_THRESHOLD)" 'BEGIN { \
		if (coverage < threshold) { \
			printf "coverage %.1f%% is below required %.1f%%\n", coverage, threshold; \
			exit 1; \
		} \
		printf "coverage %.1f%% meets required %.1f%%\n", coverage, threshold; \
	}'

check: lint test
	@govulncheck ./...
	@go fix -diff ./...

update:
	@go get -u
	@go mod tidy
