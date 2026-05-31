## ADDED Requirements

### Requirement: Schema includes a table with quoted-identifier names
`testdata/test_schema.sql` SHALL define at least one table whose name and at least two of whose column names require double-quoting because they contain spaces (e.g. `"quoted table name"`, `"col with spaces"`). The table SHALL include at least a text column and an integer column, plus a primary key.

#### Scenario: Quoted table is queryable
- **WHEN** `SELECT * FROM "quoted table name"` is run against the test database after schema load
- **THEN** the query succeeds and returns the expected columns

#### Scenario: Column names contain spaces
- **WHEN** `SELECT column_name FROM information_schema.columns WHERE table_name = 'quoted table name'` is run
- **THEN** at least two returned column names contain a space character

