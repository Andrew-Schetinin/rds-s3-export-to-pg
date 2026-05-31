## 1. Update docker_test.sh

- [ ] 1.1 In `testdata/docker_test.sh`, add a `18)` case with the correct `postgis/postgis:18-3.5` image (verify tag exists on Docker Hub before using) and remove the `14)` case
- [ ] 1.2 Add `--tmpfs /var/lib/postgresql/data:rw,size=256m` to the `docker run` invocation in `testdata/docker_test.sh`; do **not** add `noexec` — it interferes with PostGIS extension loading

## 2. Reusable Matrix Workflow

- [ ] 2.1 Create `.github/workflows/test-postgres-matrix.yml` with `workflow_call` trigger, `permissions: {}` at the workflow level, and a matrix job over `pg-version: [15, 16, 17, 18]` with `fail-fast: false`
- [ ] 2.2 In each matrix job step, run `./testdata/docker_test.sh ${{ matrix.pg-version }}` and verify exit code propagates correctly
- [ ] 2.3 Add `timeout-minutes: 10` to the matrix job to prevent hung Docker containers from consuming runners for hours

## 3. Update CI Workflow

- [ ] 3.1 In `.github/workflows/go.yml`, add a `test-postgres-matrix` job that calls the reusable workflow via `uses: ./.github/workflows/test-postgres-matrix.yml`
- [ ] 3.2 Add `if: github.event.pull_request.draft == false` condition to the matrix job so it skips for draft PRs
- [ ] 3.3 Confirm `go.yml` does NOT have a tag trigger (matrix on tags is handled by `release.yml` only)
- [ ] 3.4 Add a `concurrency` group to `go.yml` (`group: ${{ github.workflow }}-${{ github.ref }}`, `cancel-in-progress: true`) so rapid commits to a PR cancel redundant in-progress runs

## 4. Update Release Workflow

- [ ] 4.1 In `.github/workflows/release.yml`, add a `test-postgres-matrix` job (calling the reusable workflow) that runs before the `release` job
- [ ] 4.2 In `.github/workflows/release.yml`, add a `go-build` job (checkout → setup-go → `go build -v ./...` → `go test -v -skip "TestCreateTestDatabase" ./...`) using the same action versions as `go.yml`; this is necessary because `go.yml` does not trigger on tag pushes and GitHub Actions cannot depend on jobs from a different workflow
- [ ] 4.3 Update the existing GoReleaser job to declare `needs: [go-build, test-postgres-matrix]` so it only runs after both jobs pass

## 5. Documentation

- [ ] 5.1 In `DEVELOPMENT.md`: (a) add a section documenting the supported PostgreSQL versions (15–18), the AWS-RDS-first selection rationale, links to https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-release-calendar.html, https://www.postgresql.org/support/versioning/, and https://hub.docker.com/r/postgis/postgis/tags, and instructions for updating the matrix when versions change; (b) update the existing `Testdata SQL Files (Docker)` section to remove PG 14 usage examples and `docker pull` commands, and add PG 18 equivalents
- [ ] 5.2 In the PR description, document the manual branch protection step required after merge (add the `test-postgres-matrix` check names to the required checks for `main`)
- [ ] 5.3 In `CLAUDE.md`, update the supported PostgreSQL version range from "14–17" to "15–18" in the build/test commands section and any other references

## 6. Validation

- [ ] 6.1 Open a non-draft PR from this branch and confirm the matrix job runs for all 4 PG versions (15–18) and passes
- [ ] 6.2 Confirm draft PR skips the matrix job (check Actions tab after converting the PR to draft)
- [ ] 6.3 After merge to main, confirm the matrix job runs on the push-to-main trigger
- [ ] 6.4 Configure branch protection: add the `test-postgres-matrix` check names as required checks for `main` in GitHub repo settings
