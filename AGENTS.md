# GitHub Actions

Every `ectobit/*` action and reusable workflow tracks `@main` and never
overrides tool versions owned by the shared workflow. Third-party actions use
bare major version tags such as `@v7`, never commit SHAs, minor or patch tags,
or `@main`.

Local lint invocations use the bare `golangci-lint run` command and contain no
version handling; the shared workflow owns the matching CI command and tool
version.
