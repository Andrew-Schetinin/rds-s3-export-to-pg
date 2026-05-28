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

Tests that interact with PostgreSQL require a local server on `localhost:5432` with the `postgres` user. Create the file `src/.test_config.yaml` (gitignored) with the password:

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
