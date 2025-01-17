package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
	db               *pgx.Conn //*sql.DB
	regExPrimary     *regexp.Regexp
	regExIdx         *regexp.Regexp
	regExCon         *regexp.Regexp
}

func NewDatabaseWriter(host string, port int, name string, user string, password string, mode bool) DatabaseWriter {
	// Compile the regular expression
	rePrimary, err := regexp.Compile(".*PRIMARY KEY.*")
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}
	reIdx, err := regexp.Compile(".*UNIQUE INDEX.*(id).*")
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}
	reCon, err := regexp.Compile(".*UNIQUE.*")
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
	}
	return DatabaseWriter{
		ConnectionString: fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			user,
			password,
			host,
			port,
			name,
			map[bool]string{true: "require", false: "disable"}[mode],
		),
		regExPrimary: rePrimary,
		regExIdx:     reIdx,
		regExCon:     reCon,
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
	db, err := pgx.Connect(context.Background(), w.ConnectionString)
	if err == nil && db == nil {
		return fmt.Errorf("database connection is nil")
	}
	w.db = db
	return err
}

func (w *DatabaseWriter) close() {
	if w.db != nil {
		logger.Debug("Closing the database connection")
		err := w.db.Close(context.Background())
		w.db = nil
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
	}
}

func (w *DatabaseWriter) getIndexList(tableName string) (ret []IndexInfo, err error) {
	//const tableName = "entity_type"
	// Query for existing indexes on a specific table
	rows, err := w.db.Query(context.Background(), findIndexes, tableName)
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
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
			logger.Error("ERROR: ", zap.Error(err))
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
		logger.Error("ERROR: ", zap.Error(err))
		return nil, err
	}

	return indexInfos, nil
}

func (w *DatabaseWriter) getConstraintList(tableName string) (ret []ConstraintInfo, err error) {
	rows, err := w.db.Query(context.Background(), findConstrains, tableName)
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
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
			logger.Error("ERROR: ", zap.Error(err))
			return nil, err
		}

		constraints = append(constraints, ConstraintInfo{
			Name:    name,
			Command: definition,
		})
	}
	if err := rows.Err(); err != nil {
		logger.Error("ERROR: ", zap.Error(err))
		return nil, err
	}
	return constraints, nil
}

func (w *DatabaseWriter) writeTable(source Source, mapper *FieldMapper) (ret int, err error) {
	start := time.Now()
	tableName := mapper.Info.TableName
	indexInfos, err := w.getIndexList(tableName)
	if err != nil {
		return
	}
	constraints, err := w.getConstraintList(tableName)
	if err != nil {
		return
	}
	// Begin a transaction
	tx, err := w.db.Begin(context.Background())
	if err != nil {
		return
	}
	defer closeTransactionInPanic(tx)

	rows, err := w.db.Query(context.Background(), deferConstraints)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	logger.Debug("deferConstraints query executed", zap.Any("rows", rows))
	rows.Close()

	rows, err = w.db.Query(context.Background(), fmt.Sprintf(disableTriggers, tableName))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	logger.Debug("deferConstraints query executed", zap.Any("rows", rows))
	rows.Close()

	err = w.dropIndexes(tableName, constraints, err, tx, indexInfos)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	ret, err = w.writeTableData(source, mapper)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	err = w.restoreIndexes(tableName, indexInfos, err, tx, constraints)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}

	rows, err = w.db.Query(context.Background(), fmt.Sprintf(enableTriggers, tableName))
	if err != nil {
		_ = tx.Rollback(context.Background())
		return
	}
	logger.Debug("deferConstraints query executed", zap.Any("rows", rows))
	rows.Close()

	err = tx.Commit(context.Background())

	recordsPerSecond := 0.0
	secondsPassed := time.Since(start).Seconds()
	if secondsPassed > 0 {
		recordsPerSecond = float64(ret) / secondsPassed
	} else if microsecondsPassed := time.Since(start).Milliseconds(); microsecondsPassed > 0 {
		x := ret * 1000000
		recordsPerSecond = float64(x) / float64(microsecondsPassed)
	}

	logger.Info("COPY TO command executed successfully",
		zap.String("table", mapper.Info.TableName),
		zap.Int("rows_copied", ret),
		zap.Duration("execution_time", time.Since(start)),
		zap.Int64("records_per_second", int64(recordsPerSecond)))

	return
}

func (w *DatabaseWriter) restoreIndexes(tableName string, indexInfos []IndexInfo, err error, tx pgx.Tx, constraints []ConstraintInfo) error {
	for _, indexInfo := range indexInfos {
		if w.regExIdx.MatchString(indexInfo.Def) {
			logger.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			logger.Info(indexInfo.Def)
			_, err = tx.Exec(context.Background(), indexInfo.Def)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}

	for _, constraint := range constraints {
		var createSql = fmt.Sprintf(addConstraint, tableName, constraint.Name, constraint.Command)
		if w.regExPrimary.MatchString(createSql) || w.regExCon.MatchString(constraint.Command) {
			logger.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			logger.Info(createSql)
			_, err = tx.Exec(context.Background(), createSql)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err))
				break
			}
		}
	}
	return err
}

func (w *DatabaseWriter) dropIndexes(tableName string, constraints []ConstraintInfo, err error, tx pgx.Tx, indexInfos []IndexInfo) error {
	for _, constraint := range constraints {
		var dropSql = fmt.Sprintf(dropConstraint, tableName, constraint.Name)
		if w.regExPrimary.MatchString(constraint.Command) {
			logger.Debug("Skipping the primary key constraint: ", zap.String("command", constraint.Command))
		} else {
			logger.Info(dropSql)
			_, err = tx.Exec(context.Background(), dropSql)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err), zap.String("command", constraint.Command))
				break
			}
		}
	}

	for _, indexInfo := range indexInfos {
		var dropSql = fmt.Sprintf(dropIndex, indexInfo.Name)
		if w.regExIdx.MatchString(indexInfo.Def) {
			logger.Debug("Skipping the unique index: ", zap.String("command", indexInfo.Def))
		} else {
			logger.Info(dropSql)
			_, err = tx.Exec(context.Background(), dropSql)
			if err != nil {
				logger.Error("ERROR: ", zap.Error(err), zap.String("command", indexInfo.Def))
				break
			}
		}
	}
	return err
}

func closeTransactionInPanic(tx pgx.Tx) {
	logger.Debug("Closing the transaction")
	if p := recover(); p != nil {
		logger.Debug("Rollback on panic")
		err := tx.Rollback(context.Background())
		if err != nil {
			logger.Warn("Rollback error during panic", zap.Error(err))
		}
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
	rows, err := w.db.Query(context.Background(), listTables)
	logger.Debug("listTables query executed", zap.Duration("execution_time", time.Since(startTime)))
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

func (w *DatabaseWriter) getFKeys() (*FKeysGraph[Relation], error) {
	// Query for foreign key constraints in all tables
	startTime := time.Now() // Start measuring time
	if w.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	logger.Debug("Querying foreign keys...")                 //, zap.String("query", listFKeys))
	rows, err := w.db.Query(context.Background(), listFKeys) // Execute the query
	logger.Debug("listFKeys query executed", zap.Duration("execution_time", time.Since(startTime)))
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys failed: %w", err)
	}
	defer func() {
		rows.Close()
	}()

	fkMap := NewFKeysGraph[Relation](1000)
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

func (w *DatabaseWriter) getFieldMapper(info ParquetFileInfo) (ret FieldMapper, err error) {
	mapper := FieldMapper{
		Info:   info,
		Writer: w,
	}
	return mapper, nil
}

func (w *DatabaseWriter) getTableSize(tableName string) int {
	size := -1
	query := fmt.Sprintf(selectTableSize, tableName)
	err := w.db.QueryRow(context.Background(), query).Scan(&size)
	if err != nil {
		logger.Error("Failed to fetch table size", zap.String("table_name", tableName), zap.Error(err))
		return -1
	}
	return size
}

func (w *DatabaseWriter) writeTableData(source Source, mapper *FieldMapper) (ret int, err error) {
	// TODO: replace the database name with a name read from the configuration
	relativePath := fmt.Sprintf("%s/%s", "tms_test", mapper.Info.TableName)
	allFiles, err := source.listFilesRecursively(relativePath)
	slices.Sort(allFiles)

	// Group files by their subfolders
	groupedFiles := make(map[string][]string) // map[subfolder][]files
	for _, file := range allFiles {
		subfolder := filepath.Dir(file) // Get the subfolder path
		groupedFiles[subfolder] = append(groupedFiles[subfolder], file)
	}

	// Process each group
	for subfolder, files := range groupedFiles {
		logger.Debug("Processing files in subfolder", zap.String("subfolder", subfolder))

		// Ensure the files list contains the "_success" file
		successFileFound := false
		for _, file := range files {
			s := filepath.Base(file)
			if s == "_success" || s == "_SUCCESS" {
				successFileFound = true
				break
			}
		}
		if !successFileFound {
			return -1, fmt.Errorf("missing _success file in subfolder: %s", subfolder)
		}

		// Process files in the subfolder group
		for _, file := range files {
			s := filepath.Base(file)
			if s == "_success" || s == "_SUCCESS" {
				logger.Debug("Skipping the _success file")
			} else if strings.HasSuffix(s, ".parquet") {
				logger.Debug("Processing file", zap.String("file", file))

				// Add specific file processing logic here
				size, err := w.writeTablePart(source, mapper, file)
				if err != nil {
					return -1, fmt.Errorf("writing table part failed: %w", err)
				}
				ret += size
			} else {
				logger.Warn("Skipping file with unsupported extension", zap.String("file", file))
			}
		}
	}

	return ret, nil
}

func (w *DatabaseWriter) writeTablePart(source Source, mapper *FieldMapper, relativePath string) (ret int, err error) {
	file := source.getFile(relativePath)

	//rows := [][]any{
	//	{"John", "Smith", int32(36)},
	//	{"Jane", "Doe", int32(29)},
	//}

	parts := strings.Split(mapper.Info.TableName, ".")
	if len(parts) != 2 {
		// Handle the error if the identifier format is invalid (e.g., missing schema or table name)
		logger.Fatal("Invalid identifier format. Expected 'schema_name.table_name'")
	}
	schemaName, tableName := parts[0], parts[1]

	copied, err := w.db.CopyFrom(
		context.Background(),
		pgx.Identifier{schemaName, tableName},
		mapper.getFieldNames(), //[]string{"first_name", "last_name", "age"},
		mapper.getRows(file),   // pgx.CopyFromRows(rows),
	)
	if err != nil {
		return -1, fmt.Errorf("writing the table '%s' failed: %w", mapper.Info.TableName, err)
	}

	ret += int(copied)

	return ret, nil
}
