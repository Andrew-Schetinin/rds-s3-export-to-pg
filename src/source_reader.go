package main

import (
	config2 "dbrestore/config"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/bcicen/jstream"
	"go.uber.org/zap"
)

// ColumnInfo represents metadata about a database column, including its name, type, and precision constraints.
type ColumnInfo struct {
	// ColumnName defines the name of the database column in the source table.
	ColumnName string `json:"columnName"`

	// OriginalType indicates the original PostgreSQL data type of the column as defined in the source database.
	OriginalType string `json:"originalType"`

	// ExpectedExportedType specifies the type in Parquet file.
	ExpectedExportedType string `json:"expectedExportedType"`

	// OriginalCharMaxLength specifies the maximum character length for the column as defined in the source database.
	OriginalCharMaxLength int `json:"originalCharMaxLength"`

	// OriginalNumPrecision defines the numeric precision of the column as specified in the source database.
	OriginalNumPrecision int `json:"originalNumPrecision"`

	// OriginalDateTimePrecision defines the precision of datetime values in the source database for this column.
	OriginalDateTimePrecision int `json:"originalDateTimePrecision"`
}

type ParquetFileInfo struct {
	TableName string
	FileName  string
	Columns   []ColumnInfo
}

func NewParquetFileInfo(tableName, fileName string, columns []ColumnInfo) ParquetFileInfo {
	return ParquetFileInfo{TableName: tableName, FileName: fileName, Columns: columns}
}

type ParquetFileInfoList []ParquetFileInfo

// SourceReader reads and parses Parquet files from the given Source
type SourceReader struct {
	// source local or remote AWS RDS exported snapshot with JSON and Parquet files
	source Source

	// config holds the application configuration, important for the parsing process.
	config *config2.Config
}

// NewSourceReader initializes a SourceReader with the given Source instance.
func NewSourceReader(config *config2.Config, source Source) SourceReader {
	return SourceReader{config: config, source: source}
}

// iterateOverTables validates export metadata and ensures all conditions on snapshot name, status, and progress are met.
func (r *SourceReader) iterateOverTables(databaseTables []string) (ret ParquetFileInfoList, err error) {
	err = r.validateExportInfo()
	if err != nil {
		return nil, err
	}

	files, err := r.listTableListFiles()
	if err != nil {
		return nil, fmt.Errorf("iterateOverTables(): %w", err)
	}

	// we need it for validating that all tables are present
	tableMap := make(map[string]bool)
	for _, table := range databaseTables {
		tableMap[table] = false
	}

	ret = make(ParquetFileInfoList, 0)
	for _, file := range files {
		moreTables, err := r.processFile(file, &tableMap)
		if err != nil {
			return nil, fmt.Errorf("iterateOverTables(): error reading the file %s: %w",
				file, err)
		}
		ret = append(ret, moreTables...)
	}

	// Iterate over the tableMap and log every table with a value of `false`.
	errorCount := 0
	for tableName, isPresent := range tableMap {
		if !isPresent {
			if r.tableIgnored(tableName) {
				log.Debug("iterateOverTables(): the table is ignored", zap.String("table name", tableName))
			} else {
				log.Error("iterateOverTables(): missing table in source files",
					zap.String("table name", tableName))
				errorCount++
			}
		}
	}

	if errorCount > 0 {
		err = fmt.Errorf("iterateOverTables(): %d errors found", errorCount)
	}
	return
}

func (r *SourceReader) processFile(relativePath string, tableMap *map[string]bool) (ret ParquetFileInfoList, err error) {
	fileInfo := r.source.getFile(relativePath)
	defer r.source.Dispose(fileInfo)
	log.Debug("processFile()", zap.String("fileInfo.localPath", fileInfo.localPath))

	// Open the JSON file for reading
	file, err := os.Open(fileInfo.localPath)
	if err != nil {
		return nil, fmt.Errorf("processFile(): failed to open file '%s': %w", fileInfo.localPath, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error("processFile(): failed to close the file", zap.String("filePath", file.Name()),
				zap.Error(err))
		}
	}(file)

	decoder := jstream.NewDecoder(file, 2)

	ret = make(ParquetFileInfoList, 0)
	errorCount := 0
	for mv := range decoder.Stream() {
		m := mv.Value.(map[string]interface{})
		_, nodeWarning := m["warningMessage"]
		_, nodeTable := m["tableStatistics"]
		if nodeWarning {
			target, targetPresent := m["target"]
			if !targetPresent || target != "postgres" {
				return nil, fmt.Errorf(
					"processFile(): error parsing the file '%s': expected 'target' = 'postgres', received: %s",
					file.Name(), target)
			}
		} else if nodeTable {
			status, statusPresent := m["status"]
			if !statusPresent || status != "COMPLETE" {
				return nil, fmt.Errorf(
					"processFile(): error parsing the file '%s': expected 'status' = 'COMPLETE', received: %s",
					file.Name(), status)
			}
			target, targetPresent := m["target"]
			if !targetPresent {
				return nil, fmt.Errorf("processFile(): error parsing the file '%s': not found node 'target'",
					file.Name())
			}
			targetStr, ok := target.(string)
			if !ok || targetStr == "" {
				return nil, fmt.Errorf(
					"processFile(): error parsing the file '%s': 'target' is not a string or is empty",
					file.Name())
			}
			schemaMetadata, schemaMetadataPresent := m["schemaMetadata"]
			if !schemaMetadataPresent {
				return nil, fmt.Errorf("processFile(): error parsing the file '%s': not found node 'schemaMetadata'",
					file.Name())
			}
			schemaMetadataMap, ok := schemaMetadata.(map[string]interface{})
			if !ok || schemaMetadataMap == nil || len(schemaMetadataMap) <= 0 {
				return nil, fmt.Errorf(
					"processFile(): error parsing the file '%s': the node 'schemaMetadata' is not a map",
					file.Name())
			}
			originalTypeMappings, originalTypeMappingsPresent := schemaMetadataMap["originalTypeMappings"]
			if !originalTypeMappingsPresent || originalTypeMappings == nil {
				return nil, fmt.Errorf(
					"processFile(): error parsing the file '%s': the node 'originalTypeMappings' is not found",
					file.Name())
			}
			originalTypeMappingsMap, ok := originalTypeMappings.([]interface{})
			if !ok || originalTypeMappingsMap == nil || len(originalTypeMappingsMap) <= 0 {
				return nil, fmt.Errorf(
					"processFile(): error parsing the file '%s': the node 'originalTypeMappings' is not a list",
					file.Name())
			}
			columnCount := len(originalTypeMappingsMap)

			// the table name is something like "database_name.schema_name.table_name" - remove the database name
			columns, err := r.readColumns(originalTypeMappingsMap)
			if err != nil {
				return nil, fmt.Errorf("processFile(): error reading columns from the file '%s': %w",
					file.Name(), err)
			}

			targetStr, err = removeDatabaseName(targetStr)
			if err != nil {
				return nil, fmt.Errorf("processFile(): error parsing the file '%s': %w", file.Name(), err)
			}

			ret = append(ret, NewParquetFileInfo(targetStr, fileInfo.localPath, columns))

			exists, ignore := r.tableFound(targetStr, tableMap)
			if exists {
				if (*tableMap)[targetStr] {
					errorCount++
					log.Error("processFile() the table is duplicate in source files",
						zap.String("table name", targetStr), zap.Int("column count", columnCount))
				} else {
					(*tableMap)[targetStr] = true
					log.Debug("processFile()", zap.String("table name", targetStr),
						zap.Int("column count", columnCount))
				}
			} else if !ignore {
				errorCount++
				log.Error("processFile() the table is not found in the database",
					zap.String("table name", targetStr), zap.Int("column count", columnCount))
			} else {
				(*tableMap)[targetStr] = true // add this table name to the set to avoid errors
				log.Debug("processFile() the table is ignored", zap.String("table name", targetStr))
			}
		}
	}

	if errorCount > 0 {
		return nil, fmt.Errorf("error parsing the file '%s': %d errors found", file.Name(), errorCount)
	}
	return ret, nil
}

func (r *SourceReader) readColumns(originalTypeMappingsMap []interface{}) (ret []ColumnInfo, err error) {
	columns := make([]ColumnInfo, 0)

	for index, item := range originalTypeMappingsMap {
		columnMap, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf(
				"readColumns(): element [%d] in 'originalTypeMappings' is not a map", index)
		}

		columnInfo := ColumnInfo{}
		columnInfo.ColumnName, err = r.readField(columnMap, index, "columnName")
		if err != nil {
			return nil, err
		}
		columnInfo.OriginalType, err = r.readField(columnMap, index, "originalType")
		if err != nil {
			return nil, err
		}
		columnInfo.ExpectedExportedType, err = r.readField(columnMap, index, "expectedExportedType")
		if err != nil {
			return nil, err
		}
		columnInfo.OriginalCharMaxLength, err = r.readIntField(columnMap, index, "originalCharMaxLength")
		if err != nil {
			return nil, err
		}
		columnInfo.OriginalNumPrecision, err = r.readIntField(columnMap, index, "originalNumPrecision")
		if err != nil {
			return nil, err
		}
		columnInfo.OriginalDateTimePrecision, err = r.readIntField(columnMap, index, "originalDateTimePrecision")
		if err != nil {
			return nil, err
		}

		columns = append(columns, columnInfo)
	}

	return columns, nil
}

func (r *SourceReader) readField(columnMap map[string]interface{}, index int, fieldName string) (val string, err error) {
	if val, exists := columnMap[fieldName].(string); exists {
		return val, nil
	}
	return "", fmt.Errorf(
		"readField(): '%s' is missing or not a string in a column in the element [%d]", fieldName, index)
}

// tableFound checks if a table exists in the provided table map and determines whether missing tables should be ignored.
func (r *SourceReader) tableFound(tableName string, tableMap *map[string]bool) (exists bool, ignore bool) {
	_, exists = (*tableMap)[tableName]
	if !exists {
		ignore = r.tableIgnored(tableName)
	}
	return exists, ignore
}

// tableIgnored checks if this missing table should be ignored
func (r *SourceReader) tableIgnored(tableName string) bool {
	// check if this missing table should be ignored
	for prefix := range r.config.IgnoreMissingTablePrefixes {
		if strings.Contains(prefix, ".") {
			if strings.HasPrefix(tableName, prefix) { // the prefix contains the schema name
				return true
			}
		} else if strings.Contains(tableName, "."+prefix) { // no schema name
			return true
		}
	}
	return false
}

func (r *SourceReader) listDatabases() error {
	err := r.validateExportInfo()
	if err != nil {
		return err
	}
	folders, err := r.source.listFiles("", "*", true)
	if err != nil || len(folders) <= 0 {
		return fmt.Errorf("error reading the database subfolders: %w", err)
	}
	log.Info(fmt.Sprintf("Found %d database folder(s)", len(folders)))
	for _, folder := range folders {
		log.Info(folder)
	}
	return nil
}

func (r *SourceReader) listTableListFiles() (files []string, err error) {
	// for example "export_tables_info_export-test-01_from_1_to_96.json"
	tablesMask := fmt.Sprintf("export_tables_info_%s_from_*.json", r.source.getSnapshotName())
	files, err = r.source.listFiles("", tablesMask, false)
	if err != nil || len(files) <= 0 {
		err = fmt.Errorf("error reading the table list: %w", err)
	} else {
		log.Debug("listTableListFiles()", zap.Int("files.len", len(files)))
	}
	return
}

func (r *SourceReader) validateExportInfo() (err error) {
	info := fmt.Sprintf("export_info_%s.json", r.source.getSnapshotName())
	exportInfoFile := r.source.getFile(info)
	log.Debug("iterateOverTables()", zap.String("exportInfoFile.localPath", exportInfoFile.localPath))
	defer r.source.Dispose(exportInfoFile)

	// Read the complete file to a string in memory
	content, err := os.ReadFile(exportInfoFile.localPath)
	if err != nil {
		return fmt.Errorf("failed to read the file '%s': %w", exportInfoFile.localPath, err)
	}
	// Load JSON as a map
	var data map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		return fmt.Errorf("failed to parse JSON from the file '%s': %w", exportInfoFile.localPath, err)
	}

	//fmt.Printf("Parsed JSON: %v\n", data)

	snapshotName := r.source.getSnapshotName()
	log.Debug("iterateOverTables()", zap.String("snapshotName", snapshotName))

	exportTaskIdentifier, ok := data["exportTaskIdentifier"]
	if !ok {
		return fmt.Errorf("key 'exportTaskIdentifier' not found in JSON data")
	}

	if exportTaskIdentifier != snapshotName {
		return fmt.Errorf("value of 'exportTaskIdentifier' does not match snapshotName: expected '%s', got '%v'",
			snapshotName, exportTaskIdentifier)
	}

	status, ok := data["status"]
	if !ok {
		return fmt.Errorf("key 'status' not found in JSON data")
	}

	const statusComplete = "COMPLETE"
	if status != statusComplete {
		return fmt.Errorf("value of 'status' does not match the expected '%s', got '%v'", statusComplete, status)
	}

	percentProgress, ok := data["percentProgress"]
	if !ok {
		return fmt.Errorf("key 'percentProgress' not found in JSON data")
	}

	const percentProgress100 = 100
	if math.Abs(percentProgress.(float64)-float64(percentProgress100)) > 0.000001 {
		return fmt.Errorf("value of 'percentProgress' does not match the expected '%d', got '%v'",
			percentProgress100, percentProgress)
	}

	return
}

func (r *SourceReader) readIntField(columnMap map[string]interface{}, index int, fieldName string) (int, error) {
	val, exists := columnMap[fieldName]
	if !exists {
		return 0, fmt.Errorf(
			"readIntField(): '%s' is missing or not a string in a column in the element [%d]", fieldName, index)
	}
	if str, ok := val.(string); ok {
		return strconv.Atoi(str)
	}
	if i, ok := val.(int); ok {
		return i, nil
	}
	if f, ok := val.(float64); ok {
		return int(f), nil
	}
	return 0, fmt.Errorf(
		"readIntField(): cannot convert '%s' field to an integer in the element [%d]", fieldName, index)
}

// removeDatabaseName removes the database name from a fully-qualified table name in the format "database.schema.table".
// It validates the input to ensure the format satisfies the expected structure containing exactly three dots.
// Returns the remaining "schema.table" string or an error if the input format is invalid.
func removeDatabaseName(targetStr string) (string, error) {
	// Validate that the string contains exactly 3 dots
	count := strings.Count(targetStr, ".")
	if count != 2 {
		return "", fmt.Errorf("removeDatabaseName(): invalid format for table name, "+
			"expected 'database_name.schema_name.table_name', got: '%s'. count = %d", targetStr, count)
	}
	// Remove the prefix up to and including the first dot
	dotIndex := strings.Index(targetStr, ".")
	if dotIndex == -1 {
		return "", fmt.Errorf("removeDatabaseName(): unable to find '.' in table name: '%s'", targetStr)
	}
	targetStr = targetStr[dotIndex+1:]
	return targetStr, nil
}
