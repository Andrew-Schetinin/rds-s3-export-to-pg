package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"time"

	// TODO: implement it later
	_ "github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type IndexInfo struct {
	Name string
	Def  string
	//Command string
}

type ConstraintInfo struct {
	Name    string
	Command string
}

// DatabaseWriter represents a utility for writing data to a database through a specified connection string.
type DatabaseWriter struct {
	//connStr := "postgres://andrews:asd@localhost:5432/tms_test?sslmode=disable"
	ConnectionString string
	db               *sql.DB
}

func NewDatabaseWriter(host string, port int, name string, user string, password string, mode bool) DatabaseWriter {
	return DatabaseWriter{
		ConnectionString: fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			user,
			password,
			host,
			port,
			name,
			map[bool]string{true: "require", false: "disable"}[mode],
		),
	}
}

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

func (w *DatabaseWriter) connect() error {
	logger.Debug("Connecting to the database")
	db, err := sql.Open("postgres", w.ConnectionString)
	if err == nil && db == nil {
		return fmt.Errorf("database connection is nil")
	}
	w.db = db
	return err
}

func (w *DatabaseWriter) close() {
	if w.db != nil {
		logger.Debug("Closing the database connection")
		err := w.db.Close()
		w.db = nil
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}
}

func (w *DatabaseWriter) writeTable(tableName string) {
	//const tableName = "entity_type"
	// Query for existing indexes on a specific table
	rows, err := w.db.Query(findIndexes, tableName)
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}(rows)

	var indexInfos []IndexInfo

	// Iterate over the rows and construct CREATE INDEX commands
	for rows.Next() {
		var indexName, indexDef string
		err = rows.Scan(&indexName, &indexDef)
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}

		indexInfo := IndexInfo{
			Name: indexName,
			Def:  indexDef,
			//Command: fmt.Sprintf("CREATE INDEX %s ON your_table_name %s;", indexName, indexDef),
		}
		indexInfos = append(indexInfos, indexInfo)
	}

	err = rows.Err()
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}

	rows, err = w.db.Query(findConstrains, tableName)
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}(rows)

	var constraints []ConstraintInfo
	for rows.Next() {
		var name, definition string
		err = rows.Scan(&name, &definition)
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}

		constraints = append(constraints, ConstraintInfo{
			Name:    name,
			Command: definition,
		})
	}
	if err := rows.Err(); err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}

	// Begin a transaction
	tx, err := w.db.Begin()
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}

	defer func() {
		if p := recover(); p != nil {
			// Rollback on panic
			err := tx.Rollback()
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err))
			}
			logger.Error("ERROR: ", zap.Any("panic", p))
		} else if err := tx.Commit(); err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}()

	// Compile the regular expression
	re, err := regexp.Compile(".*PRIMARY KEY.*")
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
		return
	}
	reIdx, err := regexp.Compile(".*UNIQUE INDEX.*(id).*")
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
		return
	}
	reCon, err := regexp.Compile(".*UNIQUE.*")
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
		return
	}

	for _, constraint := range constraints {
		var dropSql = fmt.Sprintf(dropConstraint, tableName, constraint.Name)
		if re.MatchString(constraint.Command) {
			logger.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			logger.Info(dropSql)
			_, err = tx.Exec(dropSql)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err), zap.String("command", constraint.Command))
				break
			}
		}
	}

	for _, indexInfo := range indexInfos {
		var dropSql = fmt.Sprintf(dropIndex, indexInfo.Name)
		if reIdx.MatchString(indexInfo.Def) {
			logger.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			logger.Info(dropSql)
			_, err = tx.Exec(dropSql)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err), zap.String("command", indexInfo.Def))
				break
			}
		}
	}

	for _, indexInfo := range indexInfos {
		if reIdx.MatchString(indexInfo.Def) {
			logger.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			logger.Info(indexInfo.Def)
			_, err = tx.Exec(indexInfo.Def)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}

	for _, constraint := range constraints {
		var createSql = fmt.Sprintf(addConstraint, tableName, constraint.Name, constraint.Command)
		if re.MatchString(createSql) || reCon.MatchString(constraint.Command) {
			logger.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			logger.Info(createSql)
			_, err = tx.Exec(createSql)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}

	logger.Info("Rolling back the transaction")
	err = tx.Rollback()
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}
}

// getTablesOrdered retrieves a list of database tables ordered by their creation dependencies.
func (w *DatabaseWriter) getTablesOrdered() (ret []string, err error) {
	logger.Debug("Getting ordered tables...")

	// this retrieves only the FK between tables, so some tables are missing
	fkMap, err := w.getFKeys()
	if err != nil {
		return
	}

	if !fkMap.isAcyclic() {
		return nil, fmt.Errorf("graph is not acyclic - cannot continue processing")
	}

	// sort in order of FK dependencies
	ret = fkMap.topologicalSort()
	logger.Debug("Tables sorted", zap.Int("table count", len(ret)))

	// Get a full list of tables, because we want to process all of them
	tables, err := w.getTables()
	if err != nil {
		return
	}
	logger.Debug("Tables retrieved from the database", zap.Int("table count", len(tables)))

	// Create a set from the sorted tables list - we need it for verifying which tables are missing
	setTablesFK := make(map[string]struct{}, len(ret)) // Create a set
	for _, tableName := range ret {
		setTablesFK[tableName] = struct{}{}
	}

	// append all missing tables to the sorted list
	for _, tableName := range tables {
		if _, exists := setTablesFK[tableName]; !exists {
			ret = append(ret, tableName)
		}
	}

	if len(ret) != len(tables) {
		return nil, fmt.Errorf("table count mismatch: sortedTables.len = %d, tables.len = %d",
			len(ret), len(tables))
	}

	// report to the log the order of the tables
	for _, tableName := range ret {
		node := fkMap.getNode(tableName)
		s := ""
		if node != nil {
			for key := range node.children {
				s += key + " "
			}
		}
		logger.Debug("Ordered table: ", zap.String("table", tableName), zap.String("children", s))
	}

	// Create a map from table names to their indices
	tableIndexMap := make(map[string]int, len(ret))
	for index, tableName := range ret {
		tableIndexMap[tableName] = index
	}

	errorCount := 0
	for _, index := range fkMap.graph {
		node := fkMap.nodes[index]
		// Check if the table exists in tableIndexMap
		if parentIndex, exists := tableIndexMap[node.name]; exists {
			for dependentTableName := range node.children {
				// Check if the dependent table exists in tableIndexMap
				if dependentIndex, exists := tableIndexMap[dependentTableName]; exists {
					// self-references are permitted
					if parentIndex <= dependentIndex && node.name != dependentTableName {
						errorCount += 1
						logger.Error("Parent table index is not larger than dependent table index",
							zap.String("parent_table", node.name),
							zap.String("dependent_table", dependentTableName),
							zap.Int("parent_index", parentIndex),
							zap.Int("dependent_index", dependentIndex),
						)
					}
				} else {
					logger.Warn("Dependent table not found in tableIndexMap",
						zap.String("dependent_table", dependentTableName),
					)
				}
			}
		} else {
			logger.Warn("Parent table not found in tableIndexMap",
				zap.String("parent_table", node.name),
			)
		}
	}
	if errorCount > 0 {
		return nil, fmt.Errorf("table order validation failed. error_count: %d", errorCount)
	}

	return
}

func (w *DatabaseWriter) getTables() (tables []string, err error) {
	// get all tables
	startTime := time.Now() // Start measuring time
	rows, err := w.db.Query(listTables)
	logger.Debug("listTables query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying tables failed: %w", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}(rows)

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

func (w *DatabaseWriter) getFKeys() (*FKeysGraph[Relation], error) {
	// Query for foreign key constraints in all tables
	startTime := time.Now() // Start measuring time
	if w.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	logger.Debug("Querying foreign keys...") //, zap.String("query", listFKeys))
	rows, err := w.db.Query(listFKeys)       // Execute the query
	logger.Debug("listFKeys query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys failed: %w", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error("ERROR: Failed to close foreign key query rows", zap.Error(err))
		}
	}(rows)

	fkMap := NewFKeysGraph[Relation](1000)
	count := 0
	for rows.Next() {
		count += 1
		var r Relation
		var foreignSchema, foreignTable, foreignColumns sql.NullString
		err := rows.Scan(&r.constraintName, &r.constraintType, &r.selfSchema, &r.selfTable, &r.selfColumns,
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

		if r.constraintType != "f" {
			continue // for now skip all constraints which are not foreign keys
		}

		parentName := fmt.Sprintf("%s.%s", r.selfSchema, r.selfTable)
		node := fkMap.getNode(parentName)
		if node == nil {
			node, err = fkMap.addNode(parentName)
			if err != nil {
				return nil, fmt.Errorf("adding node failed: %w", err)
			}
		}

		childName := fmt.Sprintf("%s.%s", r.foreignSchema, r.foreignTable)
		node.addChild(childName, r)
	}
	logger.Debug("listFKeys query", zap.Int("row count", count),
		zap.Int("nodes count", fkMap.getNodeCount()), zap.Int("map size", fkMap.getGraphSize()))

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating foreign key rows failed: %w", err)
	}

	// initialize in-degree values
	fkMap.calculateInDegree()

	return &fkMap, nil
}
