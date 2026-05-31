## 1. Update docker_test.sh

- [x] 1.1 In `testdata/docker_test.sh`, add a `18)` case and remove the `14)` case; before writing any image tag, verify all four tags exist on Docker Hub: `postgis/postgis:15-3.4`, `postgis/postgis:16-3.4`, `postgis/postgis:17-3.5`, `postgis/postgis:18-3.5`
- [x] 1.2 Add `--tmpfs /var/lib/postgresql/data:rw,size=256m` to the `docker run` invocation in `testdata/docker_test.sh`; do **not** add `noexec` — it interferes with PostGIS extension loading

## 2. Reusable Matrix Workflow

- [x] 2.1 Create `.github/workflows/test-postgres-matrix.yml` with `workflow_call` trigger, `permissions: {}` at the workflow level, and a matrix job over `pg-version: [15, 16, 17, 18]` with `fail-fast: false`; include an `actions/checkout` step before the test script step so the repository files are present on the runner
- [x] 2.2 In each matrix job step, run `./testdata/docker_test.sh ${{ matrix.pg-version }}` and verify exit code propagates correctly
- [x] 2.3 Add `timeout-minutes: 10` to the matrix job to prevent hung Docker containers from consuming runners for hours

## 3. Update CI Workflow

- [x] 3.1 In `.github/workflows/go.yml`, add a `test-postgres-matrix` job that calls the reusable workflow via `uses: ./.github/workflows/test-postgres-matrix.yml`
- [x] 3.2 Add `if: github.event_name != 'pull_request' || !github.event.pull_request.draft` condition to the matrix job so it skips for draft PRs but still runs on push-to-`main` (using `github.event.pull_request.draft == false` is unsafe: the property is absent on `push` events, making the expression evaluate to empty-string, which may skip the job)
- [x] 3.3 Add a `concurrency` group to `go.yml` (`group: ${{ github.workflow }}-${{ github.ref }}`, `cancel-in-progress: true`) so rapid commits to a PR cancel redundant in-progress runs

## 4. Update Release Workflow

- [x] 4.1 In `.github/workflows/release.yml`, add a `test-postgres-matrix` job (calling the reusable workflow) that runs before the `release` job
- [x] 4.2 In `.github/workflows/release.yml`, add a `go-build` job (checkout → setup-go → `go build -v ./...` → `go test -v -skip "TestCreateTestDatabase" ./...`) using the same action versions as `go.yml`; this is necessary because `go.yml` does not trigger on tag pushes and GitHub Actions cannot depend on jobs from a different workflow
- [x] 4.3 Update the existing GoReleaser job to declare `needs: [go-build, test-postgres-matrix]` so it only runs after both jobs pass

## 5. Documentation

- [x] 5.1 In `DEVELOPMENT.md`: (a) add a section documenting the supported PostgreSQL versions (15–18), the AWS-RDS-first selection rationale, links to https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-release-calendar.html, https://www.postgresql.org/support/versioning/, and https://hub.docker.com/r/postgis/postgis/tags, and instructions for updating the matrix when versions change; (b) update the existing `Testdata SQL Files (Docker)` section to remove PG 14 usage examples and `docker pull` commands, and add PG 18 equivalents
- [x] 5.2 In `CLAUDE.md`, update the supported PostgreSQL version range from "14–17" to "15–18" in the build/test commands section and any other references

## 6. Validation

- [ ] 6.1 Open a non-draft PR from this branch and confirm the matrix job runs for all 4 PG versions (15–18) and passes
- [ ] 6.2 Confirm draft PR skips the matrix job (check Actions tab after converting the PR to draft)
- [ ] 6.3 After merge to main, confirm the matrix job runs on the push-to-main trigger
- [ ] 6.4 Configure branch protection: add the `test-postgres-matrix` check names as required checks for `main` in GitHub repo settings (manual step — GitHub only lists check names it has seen run against a PR targeting `main`, so this must be done after the first successful run post-merge)
- [ ] 6.5 Verify tag-push routing: after pushing a `v*` test tag (or inspecting the Actions tab), confirm that `go.yml` does NOT trigger a PostgreSQL matrix run and that `release.yml` triggers exactly one matrix run
- [ ] 6.6 Verify release gate: confirm that if any matrix job fails (e.g., by temporarily breaking `docker_test.sh` on a test branch), the GoReleaser job in `release.yml` does not run
