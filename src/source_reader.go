package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/bcicen/jstream"
	"go.uber.org/zap"
)

type ParquetFileInfo struct {
	TableName string
	FileName  string
}

func NewParquetFileInfo(tableName, fileName string) ParquetFileInfo {
	return ParquetFileInfo{TableName: tableName, FileName: fileName}
}

type ParquetFileInfoList []ParquetFileInfo

// SourceReader reads and parses Parquet files from the given Source
type SourceReader struct {
	// source local or remote AWS RDS exported snapshot with JSON and Parquet files
	source Source

	// config holds the application configuration, important for the parsing process.
	config *Config
}

// NewSourceReader initializes a SourceReader with the given Source instance.
func NewSourceReader(config *Config, source Source) SourceReader {
	return SourceReader{config: config, source: source}
}

// iterateOverTables validates export metadata and ensures all conditions on snapshot name, status, and progress are met.
func (r SourceReader) iterateOverTables(databaseTables []string) (ret ParquetFileInfoList, err error) {
	info := fmt.Sprintf("export_info_%s.json", r.source.getSnapshotName())
	exportInfoFile := r.source.getFile(info)
	logger.Debug("iterateOverTables()", zap.String("exportInfoFile.localPath", exportInfoFile.localPath))
	defer r.source.Dispose(exportInfoFile)

	// Read the complete file to a string in memory
	content, err := os.ReadFile(exportInfoFile.localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read the file '%s': %w", exportInfoFile.localPath, err)
	}
	// Load JSON as a map
	var data map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON from the file '%s': %w", exportInfoFile.localPath, err)
	}

	//fmt.Printf("Parsed JSON: %v\n", data)

	snapshotName := r.source.getSnapshotName()
	logger.Debug("iterateOverTables()", zap.String("snapshotName", snapshotName))

	exportTaskIdentifier, ok := data["exportTaskIdentifier"]
	if !ok {
		return nil, fmt.Errorf("key 'exportTaskIdentifier' not found in JSON data")
	}

	if exportTaskIdentifier != snapshotName {
		return nil,
			fmt.Errorf("value of 'exportTaskIdentifier' does not match snapshotName: expected '%s', got '%v'",
				snapshotName, exportTaskIdentifier)
	}

	status, ok := data["status"]
	if !ok {
		return nil, fmt.Errorf("key 'status' not found in JSON data")
	}

	const statusComplete = "COMPLETE"
	if status != statusComplete {
		return nil,
			fmt.Errorf("value of 'status' does not match the expected '%s', got '%v'", statusComplete, status)
	}

	percentProgress, ok := data["percentProgress"]
	if !ok {
		return nil, fmt.Errorf("key 'percentProgress' not found in JSON data")
	}

	const percentProgress100 = 100
	if math.Abs(percentProgress.(float64)-float64(percentProgress100)) > 0.000001 {
		return nil,
			fmt.Errorf("value of 'percentProgress' does not match the expected '%d', got '%v'",
				percentProgress100, percentProgress)
	}

	// for example "export_tables_info_export-test-01_from_1_to_96.json"
	tables := fmt.Sprintf("export_tables_info_%s_from_*.json", r.source.getSnapshotName())
	files, err := r.source.listFiles("", tables)
	if err != nil || len(files) <= 0 {
		return nil, fmt.Errorf("error reading the table list: %w", err)
	}
	logger.Debug("iterateOverTables()", zap.Int("files.len", len(files)))

	// we need it for validating that all tables are present
	tableMap := make(map[string]bool)
	for _, table := range databaseTables {
		tableMap[table] = false
	}

	ret = make(ParquetFileInfoList, 0)
	for _, file := range files {
		moreTables, err := r.processFile(file, &tableMap)
		if err != nil {
			return nil, fmt.Errorf("error reading the file %s: %w", file, err)
		}
		ret = append(ret, moreTables...)
	}

	// Iterate over the tableMap and log every table with a value of `false`.
	errorCount := 0
	for tableName, isPresent := range tableMap {
		if !isPresent {
			if r.tableIgnored(tableName) {
				logger.Debug("iterateOverTables() the table is ignored", zap.String("table name", tableName))
			} else {
				logger.Error("iterateOverTables() missing table in source files",
					zap.String("table name", tableName))
				errorCount++
			}
		}
	}

	if errorCount > 0 {
		return nil, fmt.Errorf("error parsing the file '%s': %d errors found", info, errorCount)
	}
	return
}

func (r SourceReader) processFile(relativePath string, tableMap *map[string]bool) (ret ParquetFileInfoList, err error) {
	fileInfo := r.source.getFile(relativePath)
	defer r.source.Dispose(fileInfo)
	logger.Debug("processFile()", zap.String("fileInfo.localPath", fileInfo.localPath))

	// Open the JSON file for reading
	file, err := os.Open(fileInfo.localPath)
	if err != nil {
		return nil, fmt.Errorf("processFile(): failed to open file '%s': %w", fileInfo.localPath, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logger.Error("processFile(): failed to close the file", zap.String("filePath", file.Name()),
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

			targetStr, err := removeDatabaseName(targetStr)
			if err != nil {
				return nil, fmt.Errorf("processFile(): error parsing the file '%s': %w", file.Name(), err)
			}

			ret = append(ret, NewParquetFileInfo(targetStr, fileInfo.localPath))

			exists, ignore := r.tableFound(targetStr, tableMap)
			if exists {
				if (*tableMap)[targetStr] {
					errorCount++
					logger.Error("processFile() the table is duplicate in source files",
						zap.String("table name", targetStr), zap.Int("column count", columnCount))
				} else {
					(*tableMap)[targetStr] = true
					logger.Debug("processFile()", zap.String("table name", targetStr),
						zap.Int("column count", columnCount))
				}
			} else if !ignore {
				errorCount++
				logger.Error("processFile() the table is not found in the database",
					zap.String("table name", targetStr), zap.Int("column count", columnCount))
			} else {
				(*tableMap)[targetStr] = true // add this table name to the set to avoid errors
				logger.Debug("processFile() the table is ignored", zap.String("table name", targetStr))
			}
		}
	}

	if errorCount > 0 {
		return nil, fmt.Errorf("error parsing the file '%s': %d errors found", file.Name(), errorCount)
	}
	return ret, nil
}

// tableFound checks if a table exists in the provided table map and determines whether missing tables should be ignored.
func (r SourceReader) tableFound(tableName string, tableMap *map[string]bool) (exists bool, ignore bool) {
	_, exists = (*tableMap)[tableName]
	if !exists {
		ignore = r.tableIgnored(tableName)
	}
	return exists, ignore
}

// tableIgnored checks if this missing table should be ignored
func (r SourceReader) tableIgnored(tableName string) bool {
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
