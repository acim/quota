.PHONY: lint test integration-test coverage check update

COVERAGE_THRESHOLD := 90.0

lint:
	@golangci-lint run

test:
	@go test -race -v ./...

integration-test:
	@go test -race -count=1 -v ./valkey

coverage:
	@go test -race -coverprofile=coverage.out .
	@coverage=$$(go tool cover -func=coverage.out | awk '/^total:/ { gsub("%", "", $$3); print $$3 }'); \
	awk -v coverage="$$coverage" -v threshold="$(COVERAGE_THRESHOLD)" 'BEGIN { \
		if (coverage < threshold) { \
			printf "coverage %.1f%% is below required %.1f%%\n", coverage, threshold; \
			exit 1; \
		} \
		printf "coverage %.1f%% meets required %.1f%%\n", coverage, threshold; \
	}'

check: lint test coverage
	@govulncheck ./...
	@go fix -diff ./...

update:
	@go get -u
	@go mod tidy
