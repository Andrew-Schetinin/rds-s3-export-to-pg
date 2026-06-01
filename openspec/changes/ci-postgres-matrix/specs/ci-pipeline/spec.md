## Existing Requirements

> These requirements describe pre-existing behavior that this change does not modify. They are included here to document the full expected state of the CI pipeline.

### Requirement: Go build and unit test job
The CI pipeline SHALL include a job that compiles the Go source and runs all unit tests (excluding database integration tests) on every push to `main` and every pull request targeting `main`.

#### Scenario: Successful build and test
- **WHEN** a commit is pushed to `main` or a PR targeting `main` is updated
- **THEN** `go build` and `go test -skip "TestCreateTestDatabase" ./...` run and must both exit 0

#### Scenario: Build or test failure blocks merge
- **WHEN** the Go build or unit tests fail on a PR
- **THEN** the PR cannot be merged until the failure is resolved
- **Prerequisite:** Branch protection must be configured to require this check (see "Documentation Updates" requirement and Task 6.4 — manual step after first successful run on `main`).

## New Requirements

### Requirement: PostgreSQL matrix job
The CI pipeline SHALL include a job that runs `./testdata/docker_test.sh` for each supported PostgreSQL major version (15, 16, 17, 18) as parallel sub-jobs within a single matrix.

#### Scenario: All versions pass
- **WHEN** `./testdata/docker_test.sh <version>` exits 0 for all four versions
- **THEN** the matrix job reports success

#### Scenario: Any version fails
- **WHEN** `./testdata/docker_test.sh <version>` exits non-zero for any version
- **THEN** the matrix job reports failure; remaining versions continue running (`fail-fast: false`)
- **Prerequisite:** Branch protection must be configured to require the matrix check names as required checks for `main` (see "Documentation Updates" requirement and Task 6.4 — manual step after first successful run on `main`).

#### Scenario: Hung container does not block runners indefinitely
- **WHEN** the PostgreSQL container fails to start or become ready within the expected time
- **THEN** the matrix job times out and fails within 10 minutes (enforced by `timeout-minutes: 10` on the job)

#### Scenario: Matrix skipped on draft PRs
- **WHEN** a pull request targeting `main` is in draft state
- **THEN** the PostgreSQL matrix job is skipped; the Go build job still runs

### Requirement: Go and PostgreSQL jobs run in parallel
The Go build/unit-test job and the PostgreSQL matrix job SHALL run concurrently — neither SHALL depend on the other completing first.

#### Scenario: Independent execution on PR
- **WHEN** a non-draft PR is updated
- **THEN** the Go job and the PG matrix job start at the same time with no ordering dependency between them

### Requirement: Reusable workflow for the matrix
The PostgreSQL matrix logic SHALL be implemented as a GitHub Actions reusable workflow (`.github/workflows/test-postgres-matrix.yml`) callable by both the CI workflow and the release workflow.

#### Scenario: Called from CI workflow
- **WHEN** `go.yml` invokes the reusable workflow
- **THEN** the matrix runs with the caller's permissions and secrets context

#### Scenario: Called from release workflow
- **WHEN** `release.yml` invokes the reusable workflow
- **THEN** the matrix runs before GoReleaser is allowed to start

### Requirement: PostgreSQL container uses tmpfs
The `testdata/docker_test.sh` script SHALL mount the PostgreSQL data directory (`/var/lib/postgresql/data`) as a tmpfs volume to eliminate disk I/O during tests.

#### Scenario: Container starts with in-memory storage
- **WHEN** `docker_test.sh` starts a PostgreSQL container
- **THEN** the container is started with `--tmpfs /var/lib/postgresql/data:rw,size=256m`

### Requirement: PostgreSQL version selection and maintenance policy
The project SHALL document which PostgreSQL major versions are tested, why they were selected, and how to update the list over time. The supported versions SHALL be those currently supported by AWS RDS, not yet past upstream EOL, and available as `postgis/postgis` Docker images.

#### Scenario: Version policy documented
- **WHEN** a developer looks up which PostgreSQL versions are supported
- **THEN** `DEVELOPMENT.md` contains the current version list, the AWS-RDS-first selection rationale, references to https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-release-calendar.html, https://www.postgresql.org/support/versioning/, and https://hub.docker.com/r/postgis/postgis/tags, and instructions for updating the matrix when versions change

#### Scenario: New PostgreSQL major version released
- **WHEN** a new PostgreSQL major version becomes available with a corresponding `postgis/postgis` Docker image
- **THEN** a follow-up change adds it to the matrix in `docker_test.sh` and the workflow, and drops the oldest EOL version

### Requirement: Trigger rules
The CI pipeline SHALL trigger jobs according to the following rules, with no duplicate runs:

| Event | Go build | PG matrix |
|---|---|---|
| Push to `main` | yes | yes |
| Non-draft PR targeting `main` | yes | yes |
| Draft PR targeting `main` | yes | no |
| Tag push (`v*`) | no | via `release.yml` only |

#### Scenario: Tag push does not trigger go.yml matrix
- **WHEN** a `v*` tag is pushed
- **THEN** `go.yml` does NOT trigger the PostgreSQL matrix (it runs once from `release.yml`)

### Requirement: Release gated on all CI jobs
The release workflow SHALL only invoke GoReleaser after both a Go build job and the PostgreSQL matrix job — both defined within `release.yml` — have passed successfully.

#### Scenario: All jobs pass on tag push
- **WHEN** a `v*` tag is pushed AND both the Go build and PG matrix jobs succeed
- **THEN** GoReleaser runs and publishes a release

#### Scenario: Any job fails on tag push
- **WHEN** a `v*` tag is pushed AND either the Go build or the PG matrix job fails
- **THEN** GoReleaser does NOT run and the release workflow fails

#### Scenario: Go build job is self-contained within release workflow
- **WHEN** a `v*` tag is pushed
- **THEN** `release.yml` runs its own Go build job; it does NOT depend on `go.yml`, which is not triggered by tag pushes

### Requirement: Reusable workflow runs with minimal permissions
The reusable workflow (`.github/workflows/test-postgres-matrix.yml`) SHALL declare `permissions: {}` at the workflow level to prevent inheriting elevated permissions from its callers.

#### Scenario: Called from release workflow with write permissions
- **WHEN** `release.yml` (which has `permissions: contents: write`) calls the reusable workflow
- **THEN** the matrix jobs run with no repository permissions, not with the caller's write access

### Requirement: Redundant PR runs are cancelled
`go.yml` SHALL configure a concurrency group keyed on workflow name and ref so that a new push to an open PR cancels any in-progress run for the same PR.

#### Scenario: Rapid commits to a PR
- **WHEN** a developer pushes multiple commits in quick succession to a PR branch
- **THEN** only the latest run proceeds; older in-progress runs for the same ref are cancelled

### Requirement: Documentation updates
The following documentation files SHALL be updated as part of this change:
- `DEVELOPMENT.md` — add a PostgreSQL version selection policy section (versions 15–18, AWS-RDS-first rationale, update instructions, and links to the AWS RDS release calendar, upstream EOL schedule, and PostGIS Docker Hub tags); update the Docker usage examples to reflect the new version range (drop PG 14, add PG 18)
- `CLAUDE.md` — update the supported PostgreSQL version range reference from "14–17" to "15–18"
- PR description — document the manual branch protection step required after merge (adding `test-postgres-matrix` check names to required checks for `main`)

#### Scenario: Developer looks up supported versions
- **WHEN** a developer opens `DEVELOPMENT.md` to find which PostgreSQL versions are tested
- **THEN** they find the current version list (15–18), the selection rationale, the three reference links, and instructions for updating the matrix when versions change

#### Scenario: CLAUDE.md version range is current
- **WHEN** a developer reads `CLAUDE.md`
- **THEN** the supported PostgreSQL version range shown in the build/test commands section reads "15–18", not "14–17"

#### Scenario: Branch protection step is discoverable
- **WHEN** a maintainer merges this PR and needs to configure branch protection
- **THEN** the PR description contains clear instructions for adding the `test-postgres-matrix` check names as required checks for `main` in GitHub repo settings
