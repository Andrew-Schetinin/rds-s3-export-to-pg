## 1. Update docker_test.sh

- [ ] 1.1 In `testdata/docker_test.sh`, add a `18)` case with the correct `postgis/postgis:18-3.5` image (verify tag exists on Docker Hub before using) and remove the `14)` case
- [ ] 1.2 Add `--tmpfs /var/lib/postgresql/data:rw,size=256m` to the `docker run` invocation in `testdata/docker_test.sh`

## 2. Reusable Matrix Workflow

- [ ] 2.1 Create `.github/workflows/test-postgres-matrix.yml` with `workflow_call` trigger and a matrix job over `pg-version: [15, 16, 17, 18]` with `fail-fast: false`
- [ ] 2.2 In each matrix job step, run `./testdata/docker_test.sh ${{ matrix.pg-version }}` and verify exit code propagates correctly

## 3. Update CI Workflow

- [ ] 3.1 In `.github/workflows/go.yml`, add a `test-postgres-matrix` job that calls the reusable workflow via `uses: ./.github/workflows/test-postgres-matrix.yml`
- [ ] 3.2 Add `if: github.event.pull_request.draft == false` condition to the matrix job so it skips for draft PRs
- [ ] 3.3 Confirm `go.yml` does NOT have a tag trigger (matrix on tags is handled by `release.yml` only)

## 4. Update Release Workflow

- [ ] 4.1 In `.github/workflows/release.yml`, add a `test-postgres-matrix` job (calling the reusable workflow) before the `release` job
- [ ] 4.2 Add `needs: test-postgres-matrix` to the existing GoReleaser job so it only runs after the matrix passes

## 5. Documentation

- [ ] 5.1 In `DEVELOPMENT.md`, add a section documenting the supported PostgreSQL versions (15–18), the AWS-RDS-first selection rationale, links to https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-release-calendar.html, https://www.postgresql.org/support/versioning/, and https://hub.docker.com/r/postgis/postgis/tags, and instructions for updating the matrix when versions change
- [ ] 5.2 In the PR description, document the manual branch protection step required after merge (add the `test-postgres-matrix` check names to the required checks for `main`)

## 6. Validation

- [ ] 6.1 Open a non-draft PR from this branch and confirm the matrix job runs for all 4 PG versions (15–18) and passes
- [ ] 6.2 Confirm draft PR skips the matrix job (check Actions tab after converting the PR to draft)
- [ ] 6.3 After merge to main, confirm the matrix job runs on the push-to-main trigger
- [ ] 6.4 Configure branch protection: add the `test-postgres-matrix` check names as required checks for `main` in GitHub repo settings
