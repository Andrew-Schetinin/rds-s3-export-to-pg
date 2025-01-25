package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
	"testing"
)

func TestCSVWriterNilAndEmptyStrings(t *testing.T) {
	// Example data to test
	data := [][]interface{}{
		{"ID", "Name", "Description"},
		{1, "Alice", nil},            // nil -> empty (no quotes)
		{2, "Bob", ""},               // empty string -> "" (quotes)
		{3, nil, nil},                // all nil
		{4, "", "Empty Description"}, // mixed: "" and valid value
		{5, nil, "one,two"},          // must be wrapped with quotes
	}

	// Write to an in-memory buffer instead of a file
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	// Write data to the CSV
	for _, row := range data {
		stringRow := make([]string, len(row))
		for i, v := range row {
			switch value := v.(type) {
			case nil:
				stringRow[i] = "" // nil -> empty, represented without quotes
			case string:
				stringRow[i] = value // Empty string or regular string
				if stringRow[i] == "" {
					stringRow[i] = NeverHappeningCharacter
				}
			default:
				stringRow[i] = toString(value) // Convert other types to string
				if stringRow[i] == "" {
					stringRow[i] = NeverHappeningCharacter
				}
			}
		}
		// Write row to the CSV
		if err := writer.Write(stringRow); err != nil {
			t.Fatalf("error writing row to CSV: %v", err)
		}
	}
	writer.Flush()

	// Check for errors during the flush process
	if err := writer.Error(); err != nil {
		t.Fatalf("error flushing CSV writer: %v", err)
	}

	// Define the expected CSV output
	expected := `ID,Name,Description
1,Alice,
2,Bob,""
3,,
4,"",Empty Description
5,,"one,two"
`

	// Compare the generated output to the expected CSV
	output := buffer.String()

	output = strings.ReplaceAll(output, NeverHappeningCharacter, "\"\"")

	if output != expected {
		t.Errorf("CSV output did not match expected result.\nExpected:\n%s\nGot:\n%s", expected, output)
	}
}

// Helper function for converting interface{} to string
func toString(value interface{}) string {
	return strings.TrimSpace(strings.Trim(fmt.Sprintf("%v", value), "\n"))
}
