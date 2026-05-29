# Type Mapper Gap Analysis

Comparison of `testdata/comprehensive_test_schema.sql` against
`src/target/field_mapper.go` (`Transform` method).

Types or features not handled by the tool are listed below.
Each is a future fix target in the tool — not an omission from the schema.

## Currently Handled (no gap)

| PostgreSQL type         | OriginalType in manifest            |
|-------------------------|-------------------------------------|
| BOOLEAN                 | `boolean`                           |
| SMALLINT                | `smallint`                          |
| INTEGER                 | `integer`                           |
| BIGINT                  | `bigint`                            |
| REAL                    | `real`                              |
| DOUBLE PRECISION        | `double precision`                  |
| NUMERIC / DECIMAL       | `numeric`                           |
| CHARACTER VARYING       | `character varying`                 |
| TEXT                    | `text`                              |
| TIMESTAMP               | `timestamp without time zone`       |
| DATE                    | `date`                              |
| JSONB                   | `jsonb`                             |
| Arrays (all element types) | `ARRAY`                          |
| USER-DEFINED (binary UTF8) | `USER-DEFINED` (CSV path only)   |

## Gaps — Type Mapper Will Panic

The following types are not handled in `Transform`. Loading a table that
contains any of these columns will cause the tool to panic at runtime.

### Scalar Types

| PostgreSQL type    | Expected OriginalType                |
|--------------------|--------------------------------------|
| CHAR               | `character`                          |
| BYTEA              | `bytea`                              |
| MONEY              | `money`                              |
| TIME               | `time without time zone`             |
| TIMETZ             | `time with time zone`                |
| TIMESTAMPTZ        | `timestamp with time zone`           |
| INTERVAL           | `interval`                           |
| UUID               | `uuid`                               |
| XML                | `xml`                                |
| JSON               | `json`                               |
| TSVECTOR           | `tsvector`                           |
| TSQUERY            | `tsquery`                            |
| BIT(n)             | `bit`                                |
| VARBIT(n)          | `bit varying`                        |
| MACADDR            | `macaddr`                            |
| MACADDR8           | `macaddr8`                           |
| INET               | `inet`                               |
| CIDR               | `cidr`                               |

### Geometric Types (native PostgreSQL, non-PostGIS)

| PostgreSQL type | Expected OriginalType |
|-----------------|-----------------------|
| POINT           | `point`               |
| LINE            | `line`                |
| LSEG            | `lseg`                |
| BOX             | `box`                 |
| PATH            | `path`                |
| POLYGON         | `polygon`             |
| CIRCLE          | `circle`              |

### Range Types

| PostgreSQL type | Expected OriginalType |
|-----------------|-----------------------|
| INT4RANGE       | `int4range`           |
| INT8RANGE       | `int8range`           |
| NUMRANGE        | `numrange`            |
| TSRANGE         | `tsrange`             |
| TSTZRANGE       | `tstzrange`           |
| DATERANGE       | `daterange`           |
| float8_range    | `USER-DEFINED`        |

### Multirange Types (PG 14+)

| PostgreSQL type   | Expected OriginalType   |
|-------------------|-------------------------|
| INT4MULTIRANGE    | `int4multirange`        |
| INT8MULTIRANGE    | `int8multirange`        |
| NUMMULTIRANGE     | `nummultirange`         |
| TSMULTIRANGE      | `tsmultirange`          |
| TSTZMULTIRANGE    | `tstzmultirange`        |
| DATEMULTIRANGE    | `datemultirange`        |

### USER-DEFINED Types (partial support)

The tool handles `USER-DEFINED` only when `ExpectedExportedType == "binary (UTF8)"`,
routing those through the CSV path. The following USER-DEFINED types are not
explicitly handled and may or may not work depending on how AWS RDS exports them:

| PostgreSQL type       | Notes                                          |
|-----------------------|------------------------------------------------|
| ENUM types            | Likely exported as strings — may work via CSV  |
| Composite types       | Complex structure — likely broken              |
| Domain types          | Depends on underlying base type                |
| Custom range types    | Likely not handled                             |
| PostGIS geometry      | Binary WKB — currently limited support (CLAUDE.md) |
| PostGIS geography     | Binary WKB — currently limited support         |
| HSTORE                | String export — works via CSV path             |

## Gaps — Structural / Feature-Level

These are not type-mapper issues but architectural gaps in the tool:

| Feature                  | Status    | Notes                                                     |
|--------------------------|-----------|-----------------------------------------------------------|
| Partitioned tables       | Not supported | Tool has no special handling for partition hierarchies |
| Table inheritance        | Unknown   | Parent/child relationship not tested                      |
| Generated columns        | Unknown   | AWS RDS may or may not export generated column values     |
| UNLOGGED tables          | Likely works | No structural difference in Parquet export            |
| Exclusion constraints    | Likely works | Constraint type doesn't affect data loading           |
| Materialized views       | N/A       | Not a table; has no Parquet export                        |
| Sequences (standalone)   | N/A       | Not a table; sequences reset separately                   |
| Schema-qualified names   | Unknown   | Tool uses `schema.table` format in DAG and COPY targets; `app` schema tables exercise this path but correct behavior is untested end-to-end |
