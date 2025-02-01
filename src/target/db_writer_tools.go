package target

import (
	"context"
	"database/sql"
	"dbrestore/dag"
	"dbrestore/utils"
	"fmt"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"time"
)

// IndexInfo represents metadata about a table index.
type IndexInfo struct {
	// Name is the name of the index.
	Name string
	// Def is the definition or creation statement of the index.
	Def string
}

// ConstraintInfo represents information about a database constraint, including its name and the command to define it.
type ConstraintInfo struct {
	// Name represents the identifier of the table constraint.
	Name string
	// Command represents the SQL definition or statement used to define the table constraint.
	Command string
}

// Relation represents a database relationship between two tables, including its details and associated schemas/tables.
// It can also be a self-reference from a table to itself.
type Relation struct {
	constraintName string
	constraintType string
	selfSchema     string
	selfTable      string
	selfColumns    string
	foreignSchema  string
	foreignTable   string
	foreignColumns string
	definition     string
}

// getIndexList retrieves a list of indexes for the specified table from the database.
// It returns a slice of IndexInfo containing index details or an error in case of failure.
func (w *DbWriter) getIndexList(tableName string) (ret []IndexInfo, err error) {
	//const tableName = "entity_type"
	// Query for existing indexes on a specific table
	rows, err := w.db.Query(context.Background(), findIndexes, tableName)
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	defer func(rows pgx.Rows) {
		rows.Close()
	}(rows)

	var indexInfos []IndexInfo

	// Iterate over the rows and construct CREATE INDEX commands
	for rows.Next() {
		var indexName, indexDef string
		err = rows.Scan(&indexName, &indexDef)
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
			return nil, err
		}

		indexInfo := IndexInfo{
			Name: indexName,
			Def:  indexDef,
			//Command: fmt.Sprintf("CREATE INDEX %s ON your_table_name %s;", indexName, indexDef),
		}
		indexInfos = append(indexInfos, indexInfo)
	}

	if err = rows.Err(); err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}

	return indexInfos, nil
}

// getConstraintList retrieves a list of constraints for a specified table from the database.
// It returns a slice of ConstraintInfo and an error if any operation fails during the query or iteration process.
func (w *DbWriter) getConstraintList(tableName string) (ret []ConstraintInfo, err error) {
	rows, err := w.db.Query(context.Background(), findConstrains, tableName)
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	defer func(rows pgx.Rows) {
		rows.Close()
	}(rows)
	var constraints []ConstraintInfo
	for rows.Next() {
		var name, definition string
		err = rows.Scan(&name, &definition)
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
			return nil, err
		}

		constraints = append(constraints, ConstraintInfo{
			Name:    name,
			Command: definition,
		})
	}
	if err := rows.Err(); err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	return constraints, nil
}

// restoreIndexes recreates database indexes and constraints for a specific table using the provided index and constraint info.
// It skips unique and primary key constraints based on specific regex patterns and executes appropriate SQL commands in a transaction.
func (w *DbWriter) restoreIndexes(tableName string, indexInfos []IndexInfo, err error, tx pgx.Tx, constraints []ConstraintInfo) error {
	for _, indexInfo := range indexInfos {
		if w.regExIdx.MatchString(indexInfo.Def) {
			log.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			log.Info(indexInfo.Def)
			_, err = tx.Exec(context.Background(), indexInfo.Def)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}

	for _, constraint := range constraints {
		var createSql = fmt.Sprintf(addConstraint, utils.SanitizeTableName(tableName), utils.SanitizeTableName(constraint.Name),
			constraint.Command)
		if w.regExPrimary.MatchString(createSql) || w.regExCon.MatchString(constraint.Command) {
			log.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			log.Info(createSql)
			_, err = tx.Exec(context.Background(), createSql)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}
	return err
}

// dropIndexes removes constraints and indexes from the specified table using the provided transaction and error handling.
func (w *DbWriter) dropIndexes(tableName string, constraints []ConstraintInfo, err error, tx pgx.Tx, indexInfos []IndexInfo) error {
	for _, constraint := range constraints {
		var dropSql = fmt.Sprintf(dropConstraint, utils.SanitizeTableName(tableName), utils.SanitizeTableName(constraint.Name))
		if w.regExPrimary.MatchString(constraint.Command) {
			log.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			log.Info(dropSql)
			_, err = tx.Exec(context.Background(), dropSql)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err), zap.String("command", constraint.Command))
				break
			}
		}
	}

	for _, indexInfo := range indexInfos {
		var dropSql = fmt.Sprintf(dropIndex, utils.SanitizeTableName(indexInfo.Name))
		if w.regExIdx.MatchString(indexInfo.Def) {
			log.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			log.Info(dropSql)
			_, err = tx.Exec(context.Background(), dropSql)
			if err != nil {
				log.Error("ERROR: ", zap.Error(err), zap.String("command", indexInfo.Def))
				break
			}
		}
	}
	return err
}

// getTables retrieves a list of all table names from the database.
// It returns a slice of table names and an error, if any occurs during the operation.
func (w *DbWriter) getTables() (tables []string, err error) {
	// get all tables
	startTime := time.Now() // Start measuring time
	rows, err := w.db.Query(context.Background(), listTables)
	log.Debug("listTables query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying tables failed: %w", err)
	}
	defer func() {
		rows.Close()
	}()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("getting columns failed: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting columns failed: %w", err)
	}

	//logger.Debug("Tables retrieved successfully", zap.Strings("tables", tables))
	return tables, nil
}

// getFKeys retrieves foreign key constraints for all tables and constructs a directed graph representing these constraints.
// Returns a graph of foreign key relationships or an error if the operation fails.
func (w *DbWriter) getFKeys() (*dag.FKeysGraph[Relation], error) {
	// Query for foreign key constraints in all tables
	startTime := time.Now() // Start measuring time
	if w.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	log.Debug("Querying foreign keys...")                    //, zap.String("query", listFKeys))
	rows, err := w.db.Query(context.Background(), listFKeys) // Execute the query
	log.Debug("listFKeys query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys failed: %w", err)
	}
	defer func() {
		rows.Close()
	}()

	fkMap := dag.NewFKeysGraph[Relation](1000)
	count := 0
	for rows.Next() {
		count += 1
		var r Relation
		var foreignSchema, foreignTable, foreignColumns sql.NullString
		var constraintType rune
		err := rows.Scan(&r.constraintName, &constraintType, &r.selfSchema, &r.selfTable, &r.selfColumns,
			&foreignSchema, &foreignTable, &foreignColumns, &r.definition)
		if err != nil {
			return nil, fmt.Errorf("scanning foreign key rows failed: %w", err)
		}
		if foreignSchema.Valid {
			r.foreignSchema = foreignSchema.String
		}
		if foreignTable.Valid {
			r.foreignTable = foreignTable.String
		}
		if foreignColumns.Valid {
			r.foreignColumns = foreignColumns.String
		}
		r.constraintType = string(constraintType)

		if r.constraintType != "f" {
			continue // for now skip all constraints which are not foreign keys
		}

		parentName := fmt.Sprintf("%s.%s", r.selfSchema, r.selfTable)
		node := fkMap.GetNode(parentName)
		if node == nil {
			node, err = fkMap.AddNode(parentName)
			if err != nil {
				return nil, fmt.Errorf("adding node failed: %w", err)
			}
		}

		childName := fmt.Sprintf("%s.%s", r.foreignSchema, r.foreignTable)
		node.AddChild(childName, r)
	}
	log.Debug("listFKeys query", zap.Int("row count", count),
		zap.Int("nodes count", fkMap.GetNodeCount()), zap.Int("map size", fkMap.GetGraphSize()))

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating foreign key rows failed: %w", err)
	}

	// initialize in-degree values
	fkMap.CalculateInDegree()

	return &fkMap, nil
}
