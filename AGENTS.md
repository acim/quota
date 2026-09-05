# GitHub Actions

Every `ectobit/*` action and reusable workflow tracks `@main` and never
overrides tool versions owned by the shared workflow. Third-party actions use
bare major version tags such as `@v7`, never commit SHAs, minor or patch tags,
or `@main`.

Local lint invocations use the bare `golangci-lint run` command and contain no
version handling; the shared workflow owns the matching CI command and tool
version.

Standard Go jobs select the latest stable release and patch. Direct
`actions/setup-go` steps use `go-version: stable` and `check-latest: true`;
shared workflows own their Go selection without consumer overrides. The
`go.mod` directive remains the minimum Go and language-semantics contract,
not the standard CI toolchain selector. Preserve module files for cache keys.
