## Context

The project currently has two GitHub Actions workflows:
- `go.yml` — builds and unit-tests on push to `main` and PRs targeting `main`
- `release.yml` — runs GoReleaser on `v*` tag pushes

Neither workflow runs the `testdata/docker_test.sh` round-trip tests, so PG-version compatibility regressions are undetected until a developer manually runs the script locally.

The Docker images are `postgis/postgis:{15,16}-3.4`, `postgis/postgis:17-3.5`, and `postgis/postgis:18-3.5` (verify tag at time of implementation). The test script takes ~30–60 s per version; all four can run in parallel, keeping total wall-clock time under 2 minutes.

## Goals / Non-Goals

**Goals:**
- Run the Docker round-trip test for PG 15, 16, 17, 18 in parallel on every relevant event
- Block PR merge if any PG version fails
- Block GoReleaser if any PG version fails
- Keep the existing lightweight Go build/unit-test job unchanged and fast

**Non-Goals:**
- Running `TestCreateTestDatabase` (needs persistent local PG instance)
- Adding new testdata scripts or changing existing ones
- Supporting PG versions outside 15–18

## Decisions

### 1. Reusable workflow for the matrix

**Decision:** Extract the PG matrix job into a [reusable workflow](https://docs.github.com/en/actions/using-workflows/reusing-workflows) (`.github/workflows/test-postgres-matrix.yml`) with a `workflow_call` trigger.

**Rationale:** Both `go.yml` and `release.yml` need to run the same matrix. A reusable workflow avoids copy-paste, keeps both callers in sync, and allows each to declare `needs: test-postgres-matrix` before their downstream jobs.

**Alternative considered:** Duplicate the matrix in both files. Rejected — two copies will drift.

### 2. Matrix strategy with `fail-fast: false`

**Decision:** Use `strategy.matrix` over `pg-version: [15, 16, 17, 18]` with `fail-fast: false`.

**Rationale:** `fail-fast: false` lets all four versions complete even if one fails, giving full diagnostic coverage in a single run. All four are independent; there is no ordering dependency.

**Note:** PG 18 support must also be added to `testdata/docker_test.sh` (extend the `case` statement with its `postgis/postgis` image tag) and PG 14 entry removed.

### 3. Trigger filtering for PRs (skip drafts)

**Decision:** In `go.yml`, keep `pull_request` triggers but add `if: github.event.pull_request.draft == false` on the matrix job (not the workflow level).

**Rationale:** Job-level conditions preserve the standard workflow activation while cheaply skipping the expensive Docker work for draft PRs. The existing Go build job can still run on drafts for fast feedback.

### 4. Tag push handled by `release.yml` only

**Decision:** Do **not** add a tag trigger to `go.yml`. The `release.yml` workflow calls the reusable matrix workflow as its first job; GoReleaser runs in a second job with `needs: test-postgres-matrix`.

**Rationale:** Adding a tag trigger to `go.yml` would create a duplicate matrix run every time a tag is pushed (once from `go.yml`, once from `release.yml`). Keeping it in `release.yml` only avoids wasted minutes.

### 5. Go build and PG matrix run in parallel

**Decision:** The Go build/unit-test job and the PostgreSQL matrix job run concurrently in `go.yml`. Neither has `needs` pointing at the other.

**Rationale:** The two jobs are fully independent — Go tests do not require a running PostgreSQL instance. Running them in parallel minimises wall-clock feedback time for developers. The release gate (`needs: [go-build, test-postgres-matrix]`) enforces that both must pass before any distribution is published, regardless of execution order.

### 6. Docker availability on `ubuntu-latest`

GitHub-hosted `ubuntu-latest` runners include Docker Engine pre-installed. `docker run` and `docker exec` work out of the box; no setup step is needed.

### 7. PostgreSQL version selection policy

**Decision:** Support PostgreSQL major versions that satisfy all three criteria: (1) supported by AWS RDS, (2) not yet past upstream EOL, and (3) have a corresponding `postgis/postgis` Docker image. In practice this means the most recent non-EOL major versions that AWS RDS has adopted.

**Rationale:** This tool exists specifically to restore data from AWS RDS Parquet exports. Testing against a PostgreSQL version that RDS does not offer is meaningless — no user will have exported data from it. The AWS RDS support list is therefore the primary filter; upstream EOL and Docker image availability are secondary constraints that rarely differ in practice.

**Current selection (as of implementation):** PG 15, 16, 17, 18.
- PG 13 reached EOL November 2025; PG 14 reaches EOL November 2026 and is excluded as it is near end of supported life.
- References:
  - AWS RDS for PostgreSQL release calendar: https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-release-calendar.html
  - PostgreSQL upstream EOL schedule: https://www.postgresql.org/support/versioning/
  - Available PostGIS Docker images: https://hub.docker.com/r/postgis/postgis/tags

**Update policy:** When a new PostgreSQL major version becomes available on AWS RDS (typically a few months after upstream release), add it to the matrix. Drop a version when AWS RDS deprecates it or it passes upstream EOL, whichever comes first. Update both `testdata/docker_test.sh` and the workflow matrix, and document the change in `DEVELOPMENT.md`.

### 8. tmpfs for PostgreSQL data directory

**Decision:** Add `--tmpfs /var/lib/postgresql/data:rw,size=256m` to the `docker run` invocation in `testdata/docker_test.sh`.

**Rationale:** Mounting the PostgreSQL data directory as an in-memory tmpfs eliminates disk I/O during container initialisation and SQL execution. The test data is tiny (well under 10 MB); 256 MB is a safe ceiling. The optimisation applies to both local runs and CI, reducing startup time measurably on cold runners.

**Note:** Do not add `noexec` to the tmpfs options — it can interfere with PostGIS extension loading in certain configurations.

## Risks / Trade-offs

- [Docker image pull time] Each matrix job pulls the `postgis/postgis` image (~400 MB) on cold start. → **Mitigation:** GitHub caches layers across runs within the same runner group; warm runs are fast. No explicit caching step is needed for correctness, only speed.
- [Free-tier minutes] Four parallel Docker jobs on `ubuntu-latest` = 4× minutes consumed per run. → **Mitigation:** The scripts are lightweight (~1 min per version); total usage remains well within free-tier limits for a low-traffic repo.
- [Reusable workflow secrets/vars] Reusable workflows inherit the caller's context by default. → No secrets are needed for these tests; no risk.

## Migration Plan

1. Create `.github/workflows/test-postgres-matrix.yml` (reusable workflow)
2. Update `.github/workflows/go.yml` to call the reusable workflow
3. Update `.github/workflows/release.yml` to call the reusable workflow and gate GoReleaser on it
4. Push to `feat/18-ci-postgres-matrix`, open PR, verify matrix runs and reports correctly
5. After merge, configure branch protection to require the matrix check (manual step in GitHub UI)

## Open Questions

- **Branch protection requires a manual step after merge.** GitHub's required status checks are configured by check name (e.g. `test-postgres-matrix / pg-15`). The protection UI only lists check names it has seen run against a PR targeting `main`. Since the new workflow file does not exist on `main` until this PR is merged, the check names are unknown to GitHub before that point. After merge, a maintainer must go to *Settings → Branches → Branch protection rules → main* and add the matrix check names to the required checks list. This step cannot be automated by the workflow itself — it must be done manually, exactly once, after the first successful run on main. Document this clearly in the PR description and in `DEVELOPMENT.md`.
