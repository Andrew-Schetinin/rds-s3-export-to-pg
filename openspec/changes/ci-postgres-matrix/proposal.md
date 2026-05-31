## Why

The existing CI pipeline only runs Go unit tests — it never exercises the full round-trip schema+seed+validate scripts against a real PostgreSQL instance. This means regressions in SQL compatibility across supported PG versions (15–18) can reach `main` undetected and block releases.

## What Changes

- Add a new GitHub Actions workflow (or extend existing) that runs `./testdata/docker_test.sh` for PG 15, 16, 17, and 18 in a parallel matrix
- Trigger the matrix on: non-draft PRs targeting `main`, every push to `main`, and every tag push (any branch)
- Gate the release workflow so GoReleaser only runs after the matrix passes
- Retain the existing lightweight Go build+unit-test job unchanged
- Document version selection criteria and update policy in `DEVELOPMENT.md`

## Capabilities

### New Capabilities

- `ci-pipeline`: The complete CI/CD pipeline spec, documenting all jobs (Go build, PostgreSQL matrix, release), trigger rules, merge gates, and optimizations. New in this change: the PostgreSQL matrix job, release gate, and tmpfs optimization.

### Modified Capabilities

(none)

## Impact

- `.github/workflows/test-postgres-matrix.yml` — new reusable workflow (with `permissions: {}` and `timeout-minutes: 10`)
- `.github/workflows/go.yml` — add matrix job call and concurrency/cancel-in-progress group
- `.github/workflows/release.yml` — add self-contained `go-build` job and `test-postgres-matrix` call; gate GoReleaser on both
- `testdata/docker_test.sh` — add PG 18 support, drop PG 14, add tmpfs mount for speed
- `DEVELOPMENT.md` — add version-policy section; update existing Docker usage examples and `docker pull` commands
- `CLAUDE.md` — update supported PostgreSQL version range from 14–17 to 15–18
- No changes to Go source code or SQL test files

## Non-Goals

- Running the Go database integration tests (`TestCreateTestDatabase`) in CI — those require a persistent local PG instance and are out of scope
- Adding new SQL test scripts or changing existing testdata SQL content
- Supporting PostgreSQL versions outside 15–18 at time of implementation (selection follows the four most recent non-EOL major versions; see design for update policy)
