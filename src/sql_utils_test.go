package main

import (
	"testing"
)

func TestCreatePgxIdentifier(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedResult string
	}{
		{
			name:           "Test simple name",
			input:          "table",
			expectedResult: `"table"`,
		},
		{
			name:           "Test name with schema",
			input:          "schema.table",
			expectedResult: `"schema"."table"`,
		},
		{
			name:           "Test wrong name",
			input:          "database.schema.table",
			expectedResult: `"database.schema.table"`,
		},
		{
			name:           "Test empty string",
			input:          "",
			expectedResult: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreatePgxIdentifier(tt.input).Sanitize()
			if result != tt.expectedResult {
				t.Errorf("CreatePgxIdentifier(%v) = %v; want %v", tt.input, result, tt.expectedResult)
			}
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedResult string
	}{
		{
			name:           "Test simple name",
			input:          "table",
			expectedResult: `"table"`,
		},
		{
			name:           "Test name with schema",
			input:          "schema.table",
			expectedResult: `"schema"."table"`,
		},
		{
			name:           "Test wrong name",
			input:          "database.schema.table",
			expectedResult: `"database.schema.table"`,
		},
		{
			name:           "Test empty string",
			input:          "",
			expectedResult: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeTableName(tt.input)
			if result != tt.expectedResult {
				t.Errorf("SanitizeTableName(%v) = %v; want %v", tt.input, result, tt.expectedResult)
			}
		})
	}
}
