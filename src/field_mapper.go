package main

import (
	"dbrestore/config"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

const ReasonNotEmpty = "Table is not empty"
const ReasonSkippedByConfig1 = "Table is not listed in --include-tables configuration"
const ReasonSkippedByConfig2 = "Table is listed in --exclude-tables configuration"

type FieldMapper struct {
	Info ParquetFileInfo

	Writer *DatabaseWriter

	Config *config.Config
}

func (m *FieldMapper) shouldSkip() (reason string, skip bool) {
	found, notEmpty := m.Config.TableNameInSet(m.Config.IncludeTables, m.Info.TableName)
	if !found && notEmpty {
		return ReasonSkippedByConfig1, true
	}
	found, notEmpty = m.Config.TableNameInSet(m.Config.ExcludeTables, m.Info.TableName)
	if found && notEmpty {
		return ReasonSkippedByConfig2, true
	}
	size := m.Writer.getTableSize(m.Info.TableName)
	if size > 0 {
		return ReasonNotEmpty, m.Config.SkipNotEmpty
	}
	return "", false
}

func (m *FieldMapper) getFieldNames() []string {
	names := make([]string, 0, len(m.Info.Columns))
	for _, column := range m.Info.Columns {
		names = append(names, column.ColumnName)
	}
	return names
}

func (m *FieldMapper) getRows(file FileInfo) *ParquetReader {
	reader := ParquetReader{
		fileInfo: file,
		mapper:   m,
	}
	return &reader
}

func (m *FieldMapper) transform(x parquet.Value) (value any, err error) {
	columnIndex := x.Column()
	column := m.Info.Columns[columnIndex]
	stringValue := x.String()
	log.Debug("transform", zap.Any("value", x), zap.String("string", stringValue),
		zap.Any("type", x.Kind()), zap.Int("columnIndex", columnIndex),
		zap.String("column", column.ColumnName), zap.String("originalType", column.OriginalType))
	if x.IsNull() {
		return nil, nil
	}
	if column.OriginalType == "boolean" {
		return x.Boolean(), nil
	}
	if column.OriginalType == "bigint" {
		return x.Int64(), nil
	}
	if column.OriginalType == "integer" {
		return x.Int32(), nil
	}
	if column.OriginalType == "smallint" {
		// there is no way to return Int16, but we assume it should not be out of bounds
		return x.Int32(), nil
	}
	if column.OriginalType == "double precision" {
		return x.Double(), nil
	}
	if column.OriginalType == "real" {
		return x.Float(), nil
	}
	if column.OriginalType == "character varying" {
		return stringValue, nil
	}
	if column.OriginalType == "text" {
		return stringValue, nil
	}
	if column.OriginalType == "timestamp without time zone" {
		return stringValue, nil
	}
	if column.OriginalType == "date" {
		return stringValue, nil
	}
	if column.OriginalType == "jsonb" {
		return stringValue, nil
	}
	if column.OriginalType == "USER-DEFINED" && column.ExpectedExportedType == "binary (UTF8)" {
		// IMPORTANT: this does not work with the binary format for HSTORE fields,
		// even though sources in Internet say it should, and therefore we must use CSV format instead
		return stringValue, nil
	}
	panic("unexpected column type: " + column.OriginalType)
	return stringValue, nil
}

// hasUserDefinedColumn checks if any column in the Parquet file has an original type of "USER-DEFINED".
// This format does not work with the binary COPY FROM by some reason, even though people say it should.
// And it forces us to fall back to CSV.
func (m *FieldMapper) hasUserDefinedColumn() bool {
	for _, column := range m.Info.Columns {
		if column.OriginalType == "USER-DEFINED" {
			return true
		}
	}
	return false
}
