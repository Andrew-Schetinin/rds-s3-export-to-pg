## ADDED Requirements

### Requirement: Schema file exists and is loadable
The project SHALL provide a single SQL DDL file at `testdata/test_schema.sql` that can be applied to a fresh PostgreSQL database (with PostGIS and HSTORE extensions) using `psql -f` without errors.

#### Scenario: Clean load
- **WHEN** `psql -f testdata/test_schema.sql` is run against an empty database with PostGIS and HSTORE available
- **THEN** all objects are created without errors and the transaction commits successfully

#### Scenario: Idempotent extension creation
- **WHEN** the file is loaded on a database that already has the required extensions installed
- **THEN** the `CREATE EXTENSION IF NOT EXISTS` statements complete without error

### Requirement: All AWS-RDS-supported scalar column types are represented
The schema SHALL contain at least one table column for every scalar PostgreSQL type that AWS RDS exports via Parquet and that the restore tool's type mapper handles, including: `SMALLINT`, `INTEGER`, `BIGINT`, `NUMERIC`/`DECIMAL`, `REAL`, `DOUBLE PRECISION`, `CHAR`, `VARCHAR`, `TEXT`, `BYTEA`, `BOOLEAN`, `DATE`, `TIME`, `TIMETZ`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`, `UUID`, `XML`, `JSON`, `JSONB`, `TSVECTOR`, `TSQUERY`, `BIT`, `VARBIT`, `MONEY`, `MACADDR`, `MACADDR8`, `INET`, `CIDR`, `POINT`, `LINE`, `LSEG`, `BOX`, `PATH`, `POLYGON`, `CIRCLE`, `INT4RANGE`, `INT8RANGE`, `NUMRANGE`, `TSRANGE`, `TSTZRANGE`, `DATERANGE`.

#### Scenario: Type coverage audit
- **WHEN** a developer queries `information_schema.columns` for the test database
- **THEN** every type listed in this requirement appears in at least one column's `data_type` or `udt_name`

### Requirement: Array columns are represented
The schema SHALL contain at least one column of an array type for each of: `INTEGER[]`, `TEXT[]`, `UUID[]`, `JSONB[]`, `NUMERIC[]`.

#### Scenario: Array column presence
- **WHEN** `information_schema.columns` is queried for columns with `data_type = 'ARRAY'`
- **THEN** at least five array columns exist across the schema

### Requirement: PostGIS types are represented
The schema SHALL contain at least one table with columns of types `geometry(Point, 4326)`, `geography(Polygon, 4326)`, and `geometry` (generic, no SRID constraint).

#### Scenario: PostGIS geometry column
- **WHEN** `geometry_columns` view is queried
- **THEN** entries for Point, Polygon, and generic geometry types are present

### Requirement: HSTORE columns are represented
The schema SHALL contain at least one table with a column of type `hstore`.

#### Scenario: HSTORE column presence
- **WHEN** `information_schema.columns` is queried for `udt_name = 'hstore'`
- **THEN** at least one row is returned

### Requirement: Custom enum types are represented
The schema SHALL define at least two `CREATE TYPE ... AS ENUM` types and use them as column types in at least one table each.

#### Scenario: Enum type usage
- **WHEN** `pg_type` is queried for `typtype = 'e'`
- **THEN** at least two enum types are present and used in table columns

### Requirement: Composite types are represented
The schema SHALL define at least one `CREATE TYPE ... AS` composite type and use it as a column type in at least one table.

#### Scenario: Composite type usage
- **WHEN** `pg_type` is queried for `typtype = 'c'` (excluding system types)
- **THEN** at least one user-defined composite type is present and used in a table column

### Requirement: Domain types are represented
The schema SHALL define at least one `CREATE DOMAIN` type and use it as a column type in at least one table.

#### Scenario: Domain type usage
- **WHEN** `pg_type` is queried for `typtype = 'd'`
- **THEN** at least one user-defined domain is present and used in a table column

### Requirement: All relationship patterns are represented
The schema SHALL include tables demonstrating: one-to-many (standard FK), one-to-one (FK with UNIQUE), many-to-many (join table), self-referential FK, and a deferred circular FK between two tables.

#### Scenario: Deferred circular FK loads without ordering
- **WHEN** data is inserted into the two circularly-referenced tables within a single deferred transaction
- **THEN** the transaction commits without FK violation errors

#### Scenario: Self-referential table structure
- **WHEN** `information_schema.referential_constraints` is queried
- **THEN** at least one constraint references the same table as its referencing table

### Requirement: All partitioning strategies are represented
The schema SHALL include: one range-partitioned table (partitioned by date) with at least two partitions, one list-partitioned table (partitioned by a code column) with at least two partitions, one hash-partitioned table with at least two partitions, and one sub-partitioned table (range → hash).

#### Scenario: Range partition count
- **WHEN** `pg_inherits` is queried for the range-partitioned parent table
- **THEN** at least two child partition tables are returned

#### Scenario: List partition count
- **WHEN** `pg_inherits` is queried for the list-partitioned parent table
- **THEN** at least two child partition tables are returned

#### Scenario: Hash partition count
- **WHEN** `pg_inherits` is queried for the hash-partitioned parent table
- **THEN** at least two child partition tables are returned

#### Scenario: Sub-partition structure
- **WHEN** `pg_inherits` is traversed two levels deep from the sub-partitioned root
- **THEN** leaf partitions exist at the second level

### Requirement: All auto-increment patterns are represented
The schema SHALL include tables using `SMALLSERIAL`, `SERIAL`, `BIGSERIAL`, `GENERATED ALWAYS AS IDENTITY`, `GENERATED BY DEFAULT AS IDENTITY`, and a standalone `CREATE SEQUENCE` referenced via `DEFAULT nextval(...)`.

#### Scenario: Identity column presence
- **WHEN** `information_schema.columns` is queried for `identity_generation IS NOT NULL`
- **THEN** at least two columns with identity generation are found (one ALWAYS, one BY DEFAULT)

#### Scenario: SERIAL columns presence
- **WHEN** `pg_class` is queried for sequences
- **THEN** sequences backing SERIAL and BIGSERIAL columns are present

### Requirement: Diverse index types are represented
The schema SHALL include at least one of each index type: B-tree (default), Hash, GIN (on JSONB or array column), GiST (on geometry or range column), SP-GiST, BRIN, and at least one partial index and one expression index.

#### Scenario: Index type coverage
- **WHEN** `pg_indexes` joined with `pg_class` and `pg_am` is queried
- **THEN** index access methods include btree, hash, gin, gist, spgist, and brin

### Requirement: Diverse constraint types are represented
The schema SHALL include at least one of each: CHECK constraint, EXCLUSION constraint (using GiST), composite UNIQUE constraint, NOT NULL constraint, DEFAULT value expression, and at least one DEFERRABLE constraint.

#### Scenario: Exclusion constraint presence
- **WHEN** `pg_constraint` is queried for `contype = 'x'`
- **THEN** at least one exclusion constraint is found

### Requirement: Views and materialized views are represented
The schema SHALL include at least one regular `VIEW` and one `MATERIALIZED VIEW` over existing tables.

#### Scenario: View presence
- **WHEN** `information_schema.views` is queried
- **THEN** at least one view is present

#### Scenario: Materialized view presence
- **WHEN** `pg_matviews` is queried
- **THEN** at least one materialized view is present

### Requirement: Triggers are represented
The schema SHALL include at least one table with a `BEFORE INSERT` trigger and one with an `AFTER UPDATE` trigger.

#### Scenario: Trigger presence
- **WHEN** `information_schema.triggers` is queried
- **THEN** at least two triggers across different event types are present

### Requirement: Stored functions and procedures are represented
The schema SHALL include at least one `CREATE FUNCTION` returning a scalar value and at least one `CREATE PROCEDURE`.

#### Scenario: Function presence
- **WHEN** `information_schema.routines` is queried for `routine_type = 'FUNCTION'`
- **THEN** at least one user-defined function is present

#### Scenario: Procedure presence
- **WHEN** `information_schema.routines` is queried for `routine_type = 'PROCEDURE'`
- **THEN** at least one user-defined procedure is present

### Requirement: Table inheritance is represented
The schema SHALL include at least one parent table with two child tables using PostgreSQL classical table inheritance (`INHERITS`).

#### Scenario: Inheritance structure
- **WHEN** `pg_inherits` is queried for the parent table
- **THEN** at least two child tables are returned
