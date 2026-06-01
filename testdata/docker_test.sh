#!/usr/bin/env bash
# =============================================================
# docker_test.sh — Schema + seed + validate round-trip test
#
# Starts a temporary postgis/postgis Docker container, applies
# all three SQL files in order, validates the result, then
# removes the container (the image is kept).
#
# USAGE:
#   ./testdata/docker_test.sh [PG_VERSION]
#
# ARGUMENTS:
#   PG_VERSION   PostgreSQL major version: 15, 16, 17, or 18.
#                Defaults to 16.
#
# EXAMPLES:
#   ./testdata/docker_test.sh       # PostgreSQL 16
#   ./testdata/docker_test.sh 18    # PostgreSQL 18
#
# REQUIREMENTS:
#   Docker must be running. Images are pulled automatically on
#   first use if not already present locally.
#
# EXIT CODES:
#   0   All steps passed.
#   1   Argument error or Docker / readiness failure.
#   3   A SQL step failed (psql ON_ERROR_STOP exit code).
# =============================================================

set -euo pipefail

# --------------- arguments ---------------
PG_VERSION="${1:-16}"

case "$PG_VERSION" in
    15) IMAGE="postgis/postgis:15-3.4" ;;
    16) IMAGE="postgis/postgis:16-3.4" ;;
    17) IMAGE="postgis/postgis:17-3.5" ;;
    18) IMAGE="postgis/postgis:18-3.6" ;;
    *)
        printf 'ERROR: unsupported PostgreSQL version "%s".\n' "$PG_VERSION" >&2
        printf '       Supported values: 15, 16, 17, 18\n' >&2
        exit 1
        ;;
esac

# --------------- config ---------------
DB_NAME="test_comprehensive"
PG_USER="postgres"
PG_PASSWORD="testpass"
CONTAINER="pg_schema_test_${PG_VERSION}_$$"   # $$ = PID keeps names unique
MAX_WAIT=30                                    # seconds to wait for readiness

# Locate SQL files relative to this script (works regardless of CWD)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --------------- cleanup ---------------
cleanup() {
    local rc=$?
    if [ $rc -ne 0 ]; then
        printf '\n=== FAILED (exit code %d) ===\n' "$rc" >&2
        if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
            printf '\n--- PostgreSQL logs ---\n' >&2
            docker logs "$CONTAINER" 2>&1 | tail -50 >&2 || true
        fi
    fi
    if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
        printf 'Removing container %s\n' "$CONTAINER"
        docker rm -f "$CONTAINER" >/dev/null
    fi
    exit $rc
}
trap cleanup EXIT

# --------------- run ---------------
printf '=== PostgreSQL %s test (%s) ===\n\n' "$PG_VERSION" "$IMAGE"

# Step 1: start container (no port mapping needed — access via docker exec)
printf '[1/5] Starting container %s\n' "$CONTAINER"
docker run -d --name "$CONTAINER" \
    -e "POSTGRES_PASSWORD=$PG_PASSWORD" \
    "$IMAGE" >/dev/null

# Step 2: wait for PostgreSQL to accept connections
printf '[2/5] Waiting for PostgreSQL to be ready'
attempt=1
while ! docker exec "$CONTAINER" pg_isready -U "$PG_USER" >/dev/null 2>&1; do
    if [ "$attempt" -ge "$MAX_WAIT" ]; then
        printf '\nERROR: PostgreSQL not ready after %d seconds\n' "$MAX_WAIT" >&2
        exit 1
    fi
    printf '.'
    sleep 1
    attempt=$((attempt + 1))
done
printf ' ready (%ds)\n' "$attempt"

# Create the test database after init scripts have finished running.
# We do NOT pass POSTGRES_DB to docker run because the postgis/postgis image's
# 10_postgis.sh init script would install PostGIS into it, and then our schema's
# CREATE EXTENSION IF NOT EXISTS postgis would hit a duplicate-key error on PG 16+.
# Creating the database here uses template1 (no PostGIS), so the schema installs
# it cleanly.
printf '[  ] Creating database %s\n' "$DB_NAME"
docker exec "$CONTAINER" createdb -U "$PG_USER" "$DB_NAME"

# Copy SQL files into the container once (avoids repeated docker cp overhead)
printf '[  ] Copying SQL files to container\n'
docker cp "$SCRIPT_DIR/test_schema.sql"   "$CONTAINER:/tmp/test_schema.sql"
docker cp "$SCRIPT_DIR/test_data.sql"     "$CONTAINER:/tmp/test_data.sql"
docker cp "$SCRIPT_DIR/test_validate.sql" "$CONTAINER:/tmp/test_validate.sql"

# Helper: run a SQL file with psql.
#   -q            suppresses informational row-count output
#   ON_ERROR_STOP causes psql to exit non-zero on the first SQL error,
#                 which then propagates through docker exec to this script
run_sql() {
    local step="$1" label="$2" file="$3"
    printf '\n[%s] %s\n' "$step" "$label"
    docker exec "$CONTAINER" \
        psql -U "$PG_USER" -d "$DB_NAME" \
             -v ON_ERROR_STOP=1 -q \
             -f "/tmp/$file"
}

run_sql "3/5" "Applying schema   (test_schema.sql)"   "test_schema.sql"
run_sql "4/5" "Loading seed data (test_data.sql)"     "test_data.sql"
run_sql "5/5" "Validation oracle (test_validate.sql)" "test_validate.sql"

printf '\n=== PASSED (PostgreSQL %s) ===\n' "$PG_VERSION"
