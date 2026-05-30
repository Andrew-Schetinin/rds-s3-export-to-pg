# test-data Specification

## Purpose
Test data is critical for ensuring that the tool can support all PostgreSQL features and edge cases. 
It must be comprehensive, representative, and easy to maintain.

## Requirements

### Requirement: Seed file exists and is loadable after the schema
The project SHALL provide `testdata/test_data.sql` that can be applied to a database that already has the comprehensive schema loaded, using `psql -f` without errors.

#### Scenario: Clean load after schema
- **WHEN** `psql -f testdata/test_data.sql` is run after `psql -f testdata/test_schema.sql`
- **THEN** all INSERT statements succeed without FK violation, type cast error, or constraint failure

### Requirement: All tables have at least one row
The seed file SHALL insert at least one row into every table defined in `test_schema.sql`. For partitioned tables, rows SHALL be inserted into the parent table (PostgreSQL routes them to the correct partition automatically); leaf partition tables are not targeted directly.

#### Scenario: Row count per base table
- **WHEN** `SELECT COUNT(*) FROM <table>` is run for each non-partition-leaf table after seeding
- **THEN** every queried table returns a count ≥ 1

#### Scenario: Partitioned parent rows routed to partitions
- **WHEN** rows are inserted into the partitioned parent tables (`events`, `regional_data`, `sharded_records`, `sub_partitioned`)
- **THEN** `SELECT COUNT(*) FROM <parent>` returns ≥ 1 and querying the appropriate partition leaf also returns ≥ 1 row

### Requirement: All scalar column types are exercised with non-NULL values
The seed file SHALL supply a non-NULL value for every scalar column in `all_scalar_types` that does not have a DEFAULT.

#### Scenario: No unexpected NULLs in all_scalar_types
- **WHEN** `SELECT * FROM all_scalar_types` is executed after seeding
- **THEN** the row contains no NULL in columns that lack a DEFAULT definition

### Requirement: FK relationships are populated correctly
The seed file SHALL insert parent rows before child rows so that all foreign key constraints are satisfied at commit time. For the circular FK between `node_a` and `node_b`, inserts SHALL be wrapped in a deferred transaction.

#### Scenario: Children reference existing parents
- **WHEN** the seed file is applied with constraints enforced (default mode)
- **THEN** no FK violation error occurs for any table except the circular-FK pair

#### Scenario: Circular FK inserted under deferred constraints
- **WHEN** `node_a` and `node_b` rows are inserted inside `BEGIN ... SET CONSTRAINTS ALL DEFERRED ... COMMIT`
- **THEN** both rows commit successfully with valid cross-references

### Requirement: Custom type columns are populated
The seed file SHALL insert rows that exercise enum, domain, and composite type columns.

#### Scenario: Enum column values
- **WHEN** rows in tables with enum columns are queried after seeding
- **THEN** each enum column contains a valid label from its enum definition

#### Scenario: Domain column values
- **WHEN** rows in tables with domain columns are queried after seeding
- **THEN** domain check constraints are satisfied (e.g., `positive_integer > 0`, `email_address` matches the pattern)

#### Scenario: Composite type column value
- **WHEN** rows in tables with composite type columns are queried
- **THEN** the composite column is non-NULL and its fields are accessible via `(col).field` syntax

### Requirement: Array columns are populated with at least one element
The seed file SHALL insert at least one non-empty array value for every array-typed column.

#### Scenario: Array element presence
- **WHEN** `array_length(<col>, 1)` is queried for each array column after seeding
- **THEN** every array column returns a length ≥ 1

### Requirement: NULL values are explicitly represented
The seed file SHALL include at least one row in `all_scalar_types` where every nullable column is set to NULL, and at least one additional row in `all_scalar_types` where every nullable column carries a non-NULL value — so that both the NULL and non-NULL paths are covered for every column.

#### Scenario: All-NULL row
- **WHEN** a row is selected from `all_scalar_types` where only `id` is non-NULL
- **THEN** every other column in that row returns NULL

#### Scenario: NULL array column
- **WHEN** a row is selected from `all_scalar_types` where `col_int_array` IS NULL
- **THEN** `array_length(col_int_array, 1)` returns NULL, confirming a NULL array (distinct from an empty array)

### Requirement: Special floating-point values are represented
The seed file SHALL insert rows in `all_scalar_types` covering `Infinity`, `-Infinity`, and `NaN` for `REAL` (`col_real`) and `DOUBLE PRECISION` (`col_double`) columns. `NUMERIC` (`col_numeric`, `col_decimal`) SHALL have a row with `NaN` (PostgreSQL `NUMERIC` supports `NaN` but not infinity).

#### Scenario: Positive infinity
- **WHEN** `SELECT col_real, col_double FROM all_scalar_types WHERE col_real = 'Infinity'` is run
- **THEN** at least one row is returned

#### Scenario: Negative infinity
- **WHEN** `SELECT col_real, col_double FROM all_scalar_types WHERE col_real = '-Infinity'` is run
- **THEN** at least one row is returned

#### Scenario: NaN float
- **WHEN** `SELECT col_real FROM all_scalar_types WHERE col_real = 'NaN'` is run
- **THEN** at least one row is returned

#### Scenario: NaN numeric
- **WHEN** `SELECT col_numeric FROM all_scalar_types WHERE col_numeric = 'NaN'` is run
- **THEN** at least one row is returned

### Requirement: Very long text values are represented
The seed file SHALL insert at least one row in `all_scalar_types` where `col_text` contains a string longer than 65,535 bytes (64 KiB), exercising PostgreSQL's ability to store large text without truncation.

#### Scenario: Large text survives round-trip
- **WHEN** `SELECT length(col_text) FROM all_scalar_types WHERE length(col_text) > 65535` is run
- **THEN** at least one row is returned and the returned length equals the length of the inserted string

### Requirement: Empty and multi-dimensional arrays are represented
The seed file SHALL insert at least one row with an empty array (`'{}'`) and at least one row with a two-dimensional array for at least one array column.

#### Scenario: Empty array
- **WHEN** `SELECT col_int_array FROM all_scalar_types WHERE col_int_array = '{}'` is run
- **THEN** at least one row is returned and `array_length(col_int_array, 1)` returns NULL (empty array has no elements)

#### Scenario: Two-dimensional array
- **WHEN** `SELECT array_ndims(col_int_array) FROM all_scalar_types WHERE array_ndims(col_int_array) = 2` is run
- **THEN** at least one row is returned

### Requirement: UTF-8 text diversity is represented
The seed file SHALL insert rows containing text values from at least four Unicode categories: emoji (e.g. U+1F600), CJK ideographs (e.g. Chinese/Japanese/Korean characters), right-to-left script characters (e.g. Arabic or Hebrew), and combining-character sequences (e.g. base letter + combining diacritic).

#### Scenario: Emoji text survives storage
- **WHEN** `SELECT col_text FROM all_scalar_types WHERE col_text LIKE '%😀%'` is run
- **THEN** at least one row is returned

#### Scenario: CJK text survives storage
- **WHEN** `SELECT col_text FROM all_scalar_types WHERE col_text LIKE '%中文%'` is run
- **THEN** at least one row is returned

#### Scenario: RTL text survives storage
- **WHEN** `SELECT col_text FROM all_scalar_types WHERE col_text LIKE '%مرحبا%'` is run
- **THEN** at least one row is returned

### Requirement: Timestamp timezone diversity is represented
The seed file SHALL insert multiple rows in `all_scalar_types` with `TIMESTAMPTZ` values expressed using at least three distinct UTC offsets (e.g. `+00:00`, `+05:30`, `-08:00`) and at least one row with a `TIMESTAMP` (no timezone) value, so that timezone parsing is exercised.

#### Scenario: Multiple UTC offsets stored
- **WHEN** `SELECT COUNT(DISTINCT EXTRACT(TIMEZONE FROM col_timestamptz)) FROM all_scalar_types` is run
- **THEN** the result is ≥ 1 (PostgreSQL normalises all TIMESTAMPTZ to UTC internally, so the test verifies at least one non-NULL value was stored; timezone-of-origin is verified at the SQL level during seeding)

#### Scenario: Bare TIMESTAMP value stored
- **WHEN** `SELECT col_timestamp FROM all_scalar_types WHERE col_timestamp IS NOT NULL` is run
- **THEN** at least one row is returned

### Requirement: PostGIS columns are populated
The seed file SHALL insert at least one row into `spatial_features` with non-NULL WKT values for all five PostGIS columns: `geom_point` (`geometry(Point,4326)`), `geom_linestring` (`geometry(LineString,4326)`), `geog_polygon` (`geography(Polygon,4326)`), `geom_generic` (generic `geometry`), and `geog_generic` (generic `geography`).

#### Scenario: PostGIS row present
- **WHEN** `SELECT COUNT(*) FROM spatial_features WHERE geom_point IS NOT NULL AND geom_linestring IS NOT NULL AND geog_polygon IS NOT NULL` is run after seeding
- **THEN** the count is ≥ 1

#### Scenario: Geometry values are valid
- **WHEN** `SELECT ST_IsValid(geom_generic), ST_IsValid(geom_point) FROM spatial_features` is run
- **THEN** all returned validity flags are TRUE

### Requirement: Quoted-identifier table is populated
The seed file SHALL insert at least one row into the quoted-identifier table added to the schema, covering its text and integer columns.

#### Scenario: Row present in quoted-identifier table
- **WHEN** `SELECT COUNT(*) FROM "quoted table name"` is run after seeding
- **THEN** the count is ≥ 1

