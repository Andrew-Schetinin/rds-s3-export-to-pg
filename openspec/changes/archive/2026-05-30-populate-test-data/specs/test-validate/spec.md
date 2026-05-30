## ADDED Requirements

### Requirement: Validation oracle file exists and is runnable after seeding
The project SHALL provide `testdata/test_validate.sql` that can be applied via `psql -f` after the schema and seed files, completing without error. The file SHALL use anonymous PL/pgSQL `DO $$ BEGIN ASSERT ..., '<message>'; END $$;` blocks to assert specific known values and counts.

#### Scenario: Clean run after seed
- **WHEN** `psql -f testdata/test_validate.sql` is run after the schema and seed files have been applied
- **THEN** all ASSERT statements pass and the file exits with code 0

#### Scenario: Failure on missing data
- **WHEN** the validate file is run against a database where the seed was NOT applied
- **THEN** at least one ASSERT raises an exception, causing `psql` to exit with a non-zero code

### Requirement: Validation covers every table including partitioned parents
The validate file SHALL assert a row count ≥ 1 for every table seeded by `test_data.sql`, including all four partitioned parent tables (`events`, `regional_data`, `sharded_records`, `sub_partitioned`) and the `"quoted table name"` quoted-identifier table.

#### Scenario: Per-table count assertions
- **WHEN** the validate file is applied to a seeded database
- **THEN** every per-table ASSERT passes, confirming no table was accidentally left empty

#### Scenario: Partitioned parent has data in at least one leaf partition
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM events) >= 1` and `(SELECT COUNT(*) FROM events_2024) >= 1` are evaluated (and equivalent assertions for other partitioned tables)
- **THEN** both the parent query and the leaf partition query return ≥ 1 row

#### Scenario: Quoted-identifier table has data
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM "quoted table name") >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers scalar type coverage
The validate file SHALL assert that at least one row in `all_scalar_types` has non-NULL values in every column that lacks a DEFAULT, confirming the fully-populated row was inserted.

#### Scenario: Fully-populated row exists
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_smallint IS NOT NULL AND col_integer IS NOT NULL AND col_bigint IS NOT NULL AND col_text IS NOT NULL AND col_boolean IS NOT NULL AND col_date IS NOT NULL AND col_uuid IS NOT NULL) >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers FK integrity
The validate file SHALL assert that FK relationships are satisfied: child rows reference existing parents, and the circular-FK pair (`node_a`/`node_b`) cross-reference each other.

#### Scenario: Children have valid parent references
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM children c JOIN parents p ON c.parent_id = p.id) >= 1` is evaluated
- **THEN** it passes

#### Scenario: Circular FK cross-reference is intact
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM node_a a JOIN node_b b ON a.ref_b = b.id) >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers custom type columns
The validate file SHALL assert that enum, domain, and composite type columns carry valid non-NULL values.

#### Scenario: Enum column holds a valid label
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_status IS NOT NULL AND col_region IS NOT NULL AND col_priority IS NOT NULL) >= 1` is evaluated
- **THEN** it passes

#### Scenario: Domain column satisfies its constraint
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_pos_int > 0 AND col_email LIKE '%@%') >= 1` is evaluated
- **THEN** it passes

#### Scenario: Composite type column is non-NULL and accessible
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_address IS NOT NULL AND (col_address).street IS NOT NULL) >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers all named array columns
The validate file SHALL assert that each of the five named array columns (`col_int_array`, `col_text_array`, `col_uuid_array`, `col_jsonb_array`, `col_numeric_array`) has at least one row with a non-empty array value.

#### Scenario: Each array column has at least one non-empty row
- **WHEN** ASSERTs of the form `(SELECT COUNT(*) FROM all_scalar_types WHERE array_length(<col>, 1) >= 1) >= 1` are evaluated for each of the five array columns
- **THEN** all five pass

### Requirement: Validation covers NULL edge cases
The validate file SHALL assert both the all-NULL row and the NULL-array-vs-empty-array distinction.

#### Scenario: All-NULL row present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_smallint IS NULL AND col_text IS NULL AND col_boolean IS NULL AND col_int_array IS NULL) >= 1` is evaluated
- **THEN** it passes

#### Scenario: NULL array distinct from empty array
- **WHEN** the ASSERTs `(SELECT COUNT(*) FROM all_scalar_types WHERE col_int_array IS NULL) >= 1` and `(SELECT COUNT(*) FROM all_scalar_types WHERE col_int_array = '{}') >= 1` are evaluated
- **THEN** both pass, confirming NULL and empty arrays coexist as distinct rows

### Requirement: Validation covers special floating-point values
The validate file SHALL assert the presence of `Infinity`, `-Infinity`, and `NaN` for float columns, and `NaN` for the numeric column.

#### Scenario: Positive infinity present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_real = 'Infinity'::real) >= 1` is evaluated
- **THEN** it passes

#### Scenario: Negative infinity present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_real = '-Infinity'::real) >= 1` is evaluated
- **THEN** it passes

#### Scenario: NaN float present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_real = 'NaN'::real) >= 1` is evaluated
- **THEN** it passes

#### Scenario: NaN numeric present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_numeric = 'NaN'::numeric) >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers long text
The validate file SHALL assert that at least one `col_text` value exceeds 65,535 bytes.

#### Scenario: Long text row present
- **WHEN** the ASSERT `(SELECT MAX(length(col_text)) FROM all_scalar_types) > 65535` is evaluated
- **THEN** it passes

### Requirement: Validation covers array edge cases
The validate file SHALL assert the presence of both an empty array and a two-dimensional array.

#### Scenario: Empty array present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_int_array = '{}') >= 1` is evaluated
- **THEN** it passes

#### Scenario: Two-dimensional array present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE array_ndims(col_int_array) = 2) >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers UTF-8 text diversity
The validate file SHALL assert the presence of text from all four required Unicode categories: emoji, CJK, RTL, and combining characters.

#### Scenario: Emoji text present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_text LIKE '%😀%') >= 1` is evaluated
- **THEN** it passes

#### Scenario: CJK text present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_text LIKE '%中文%') >= 1` is evaluated
- **THEN** it passes

#### Scenario: RTL text present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_text LIKE '%مرحبا%') >= 1` is evaluated
- **THEN** it passes

#### Scenario: Combining-character text present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_text LIKE '%café%') >= 1` is evaluated (where `é` is stored as base `e` + combining accent U+0301)
- **THEN** it passes

### Requirement: Validation covers timestamp and timezone diversity
The validate file SHALL assert the presence of at least one non-NULL `TIMESTAMPTZ` value and at least one non-NULL bare `TIMESTAMP` value.

#### Scenario: TIMESTAMPTZ value present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_timestamptz IS NOT NULL) >= 3` is evaluated (one per UTC offset)
- **THEN** it passes

#### Scenario: Bare TIMESTAMP value present
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM all_scalar_types WHERE col_timestamp IS NOT NULL) >= 1` is evaluated
- **THEN** it passes

### Requirement: Validation covers PostGIS data
The validate file SHALL assert that `spatial_features` has at least one row with all five PostGIS columns non-NULL and that the geometry values are valid.

#### Scenario: PostGIS row present with all columns non-NULL
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM spatial_features WHERE geom_point IS NOT NULL AND geom_linestring IS NOT NULL AND geog_polygon IS NOT NULL AND geom_generic IS NOT NULL AND geog_generic IS NOT NULL) >= 1` is evaluated
- **THEN** it passes

#### Scenario: Geometry values are valid
- **WHEN** the ASSERT `(SELECT COUNT(*) FROM spatial_features WHERE NOT ST_IsValid(geom_generic)) = 0` is evaluated
- **THEN** it passes, confirming no invalid geometry was inserted

### Requirement: Validation file is reusable as a post-restore oracle
The validate file SHALL make no assumptions about how data arrived in the database (direct INSERT vs. Parquet restore), so that it can serve as the acceptance test for the future end-to-end restore pipeline without modification.

#### Scenario: File applies to a restore-populated database
- **WHEN** the validate file is run against a database populated by a future Parquet restore of the seed data
- **THEN** all assertions pass, confirming the restore preserved all values including edge cases
