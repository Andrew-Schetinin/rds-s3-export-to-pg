package main

import (
	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

const ReasonNotEmpty = "Table is not empty"

type FieldMapper struct {
	Info ParquetFileInfo

	Writer *DatabaseWriter
}

func (m *FieldMapper) shouldSkip() (reason string, skip bool) {
	size := m.Writer.getTableSize(m.Info.TableName)
	if size > 0 {
		return ReasonNotEmpty, false
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

func (m *FieldMapper) getRows(file FileInfo) pgx.CopyFromSource {
	reader := ParquetReader{
		fileInfo: file,
		mapper:   m,
	}
	return &reader
}

func (m *FieldMapper) transform(x parquet.Value) (value any, err error) {
	columnIndex := x.Column()
	column := m.Info.Columns[columnIndex]
	s := x.String()
	logger.Debug("transform", zap.Any("value", x), zap.String("string", s),
		zap.Any("type", x.Kind()), zap.Int("columnIndex", columnIndex),
		zap.String("column", column.ColumnName), zap.String("originalType", column.OriginalType))
	if x.IsNull() {
		return nil, nil
	}
	if column.OriginalType == "bigint" {
		return x.Int64(), nil
	}
	if column.OriginalType == "integer" {
		return x.Int32(), nil
	}
	if column.OriginalType == "character varying" {
		return x.String(), nil
	}
	if column.OriginalType == "timestamp without time zone" {
		//panic("unexpected column type: timestamp without time zone")
		return x.String(), nil
	}
	if column.OriginalType == "jsonb" {
		//panic("unexpected column type: jsonb")
		return x.String(), nil
	}
	panic("unexpected column type: " + column.OriginalType)
	return s, nil
}
