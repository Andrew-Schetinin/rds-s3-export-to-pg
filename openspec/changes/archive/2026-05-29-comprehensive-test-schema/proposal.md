## Why

The tool's type mapping, constraint handling, and data loading logic need to be validated against the full range of PostgreSQL features supported by AWS RDS. Without a comprehensive test schema, gaps in type coverage, unsupported partitioning strategies, and unhandled relationship patterns remain invisible — making it impossible to know what the tool needs to be fixed to support. The schema is the source of truth for what PostgreSQL can do; the tool should be fixed to match it, not the other way around.

## What Changes

- New SQL file that creates a test PostgreSQL database schema covering all AWS-RDS-supported PostgreSQL features
- Includes all standard column data types (numeric, text, temporal, binary, network, geometric, UUID, ranges, arrays, composite types)
- Includes advanced types: PostGIS geometry/geography, JSONB, JSON, HSTORE, TSVECTOR, XML, BIT/VARBIT
- Includes all relationship patterns: one-to-one, one-to-many, many-to-many, self-referential, circular FK (deferred), polymorphic-style
- Includes all partitioned table styles: range, list, hash partitioning; sub-partitioning
- Includes all auto-increment patterns: SERIAL, BIGSERIAL, IDENTITY (ALWAYS/BY DEFAULT), sequences with custom increments
- Includes diverse index types: B-tree, Hash, GIN, GiST, SP-GiST, BRIN; partial and expression indexes
- Includes check constraints, exclusion constraints, unique constraints, deferred constraints
- Includes triggers, views, materialized views, stored procedures and functions
- Includes table inheritance (classic PostgreSQL inheritance)
- Includes enum types, domain types, composite types, range types

## Capabilities

### New Capabilities

- `test-schema`: An SQL DDL file that exercises the full breadth of AWS-RDS-supported PostgreSQL features, serving as the canonical reference for what the restore tool must eventually support. It defines the target, not what is currently supported.

### Modified Capabilities

## Impact

- New file: `testdata/comprehensive_test_schema.sql`
- No changes to existing Go code
- Enables future integration tests that load data into this schema to validate end-to-end restore correctness
- Requires PostgreSQL with PostGIS extension available in the test environment
