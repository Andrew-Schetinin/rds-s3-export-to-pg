# Development Guide

## Prerequisites

- [Go](https://go.dev/) — version specified in `src/go.mod`
- [Volta](https://volta.sh/) + [Bun](https://bun.sh/) — manages Node.js and installs OpenSpec and Husky
- [golangci-lint](https://golangci-lint.run/) — required by the pre-commit hook

```bash
brew install go volta bun golangci-lint
```

## First-time setup

```bash
volta install bun
bun install        # installs OpenSpec and Husky; activates git hooks automatically
```

## Build

```bash
cd src
rm -f ./dbrestore  # remove stale binary — build silently keeps the old one on error
go build
```

## Testing

```bash
cd src
go test -v ./...                                      # all tests
go test -v -skip "TestCreateTestDatabase" ./...       # skip database-dependent tests (used in CI)
```

Go tests that interact with PostgreSQL require a local server on `localhost:5432` with the `postgres` user. Create the file `src/.test_config.yaml` (gitignored) with the password:

```yaml
password: YOUR_PASSWORD
```

Use an empty string if the user has no password:

```yaml
password: ""
```

If the `postgres` role does not exist on your local installation, create it:

```sql
CREATE ROLE postgres WITH LOGIN SUPERUSER CREATEDB CREATEROLE INHERIT NOREPLICATION;
```

## Testdata SQL Files (Docker)

The `testdata/` directory contains three SQL files that must be applied in order:

| File | Purpose |
|------|---------|
| `test_schema.sql` | Complete PostgreSQL schema (enums, tables, partitions, extensions, …) |
| `test_data.sql`   | Seed data for every table including edge-case values |
| `test_validate.sql` | Validation oracle — asserts expected data is present |

Use the helper script to run the full cycle in a temporary Docker container (no local PostgreSQL needed):

```bash
./testdata/docker_test.sh        # PostgreSQL 16 (default)
./testdata/docker_test.sh 15     # PostgreSQL 15
./testdata/docker_test.sh 17     # PostgreSQL 17
./testdata/docker_test.sh 18     # PostgreSQL 18
```

The script starts a `postgis/postgis` container, runs all three files in order with `psql`, checks exit codes at each step, then removes the container (the image is kept). It exits non-zero on any failure. Images are pulled automatically on first use.

## PostgreSQL Version Policy

The project tests against PostgreSQL major versions **15, 16, 17, and 18**. These are selected using the following criteria (all three must be satisfied):

1. **Supported by AWS RDS** — this tool exists to restore data from AWS RDS Parquet exports, so testing against versions RDS doesn't offer is meaningless
2. **Not yet past upstream EOL** — versions past their PostgreSQL community end-of-life date are excluded
3. **Available as a `postgis/postgis` Docker image** — required for the local and CI test scripts

In practice this means the four most recent non-EOL major versions that AWS RDS has adopted. PG 14 is excluded as it approaches EOL in November 2026.

**References:**
- [AWS RDS for PostgreSQL release calendar](https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-release-calendar.html)
- [PostgreSQL upstream EOL schedule](https://www.postgresql.org/support/versioning/)
- [Available PostGIS Docker images](https://hub.docker.com/r/postgis/postgis/tags)

**When to update the matrix:** When a new PostgreSQL major version becomes available on AWS RDS and has a corresponding `postgis/postgis` Docker image, add it to `testdata/docker_test.sh` (new `case` entry) and to the `pg-version` matrix in `.github/workflows/test-postgres-matrix.yml`. Drop the oldest version when AWS RDS deprecates it or it passes upstream EOL, whichever comes first. Document the change in this section and in `CLAUDE.md`.

## Dependency management

```bash
cd src
go mod tidy         # update go.mod and go.sum after changing dependencies
go get -u           # upgrade all dependencies to latest versions
govulncheck ./...   # check for known vulnerabilities
```

## OpenSpec

This project uses [OpenSpec](https://github.com/fission-ai/openspec) (`@fission-ai/openspec`) for spec-driven development. Node.js is only needed for this — there is no TypeScript code in the repo.

```bash
bunx openspec
```

- `openspec/config.yaml` — project context and per-artifact rules passed to the AI
- `openspec/specs/` — generated spec artifacts
- `openspec/changes/` — change records (archived under `changes/archive/`)

## Git workflow

### Commit format

Every commit must follow this format, enforced by the `commit-msg` hook:

```
type(#ticket): description
```

Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `perf`, `style`, `build`, `revert`.

Examples:
```
feat(#42): add S3 source support
fix(#87): handle nil pointer in parquet reader
```

### Pre-commit hook

On every commit, Husky runs:

1. `gofmt -l ./src/` — fails if any file needs formatting; run `gofmt -w ./src/` to fix
2. `golangci-lint run ./...` from `src/` — configuration in `.golangci.yaml`

### Releases

Releases are automated via goreleaser. Push a tag to trigger `.github/workflows/release.yml`:

```bash
git tag v0.2.0
git push origin v0.2.0
```

This builds binaries for Linux, macOS, and Windows (amd64 + arm64), packages them as `.tar.gz`/`.zip`, generates `checksums.txt`, and publishes a GitHub Release with a changelog. Commits of type `chore`, `docs`, `ci`, and `test` are excluded from the changelog.
