package main

import (
	"context"
	"dbrestore/config"
	"dbrestore/utils"
	"fmt"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
	_ "gopkg.in/yaml.v3"
	"io"
	"math/rand"
	"os"

	"testing"
)

const testConfigFileName = ".test_config.yaml"

const passwordKey = "password"

const localConnectionString = "postgresql://postgres:%s@localhost:5432/postgres"

const testDatabaseNamePrefix = "test_database_"

const localTestConnectionString = "postgresql://postgres:%s@localhost:5432/%s"

func loadTestConfig() map[string]interface{} {
	// Open the YAML file
	file, err := os.Open(testConfigFileName)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	// Decode the YAML into a map
	data := make(map[string]interface{})
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	return data
}

func TestCreateTestDatabase(t *testing.T) {
	conf := loadTestConfig()

	t.Run("Create test database", func(t *testing.T) {
		// initialize configuration
		pwd := conf[passwordKey].(string)
		if pwd == "" {
			t.Errorf("Local PostgreSQL password not found in the test config file: %s", testConfigFileName)
		}
		conStr := fmt.Sprintf(localConnectionString, pwd)

		// connect to PostgreSQL default database (to be able to create a new test database)
		db, err := pgx.Connect(context.Background(), conStr)
		if err != nil {
			t.Errorf("TestCreateTestDatabase() error: %v", err)
		}
		defer func(db *pgx.Conn, ctx context.Context) {
			err := db.Close(ctx)
			if err != nil {
				panic(err)
			}
		}(db, context.Background())

		// create a test database

		// Append a random number to the testDatabaseNamePrefix
		randomSuffix := fmt.Sprintf("%d", 1000+rand.Intn(9000))
		testDatabaseName := testDatabaseNamePrefix + randomSuffix

		// Attempt to create the test database
		_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s;", testDatabaseName))
		if err != nil {
			t.Errorf("Failed to create test database: %v", err)
			return
		}
		t.Logf("Test database '%s' created successfully", testDatabaseName)

		// Ensure the test database is dropped after the test finishes
		defer func() {
			_, err = db.Exec(context.Background(), fmt.Sprintf("DROP DATABASE %s;", testDatabaseName))
			if err != nil {
				t.Errorf("Failed to drop test database '%s': %v", testDatabaseName, err)
			} else {
				t.Logf("Test database '%s' dropped successfully", testDatabaseName)
			}
		}()

		runTestInAnotherDatabase(t, testDatabaseName, pwd)
	})
}

func runTestInAnotherDatabase(t *testing.T, testDatabaseName string, pwd string) {
	// Construct a new connection string specific to the test database
	testDbConnectionString := fmt.Sprintf(localTestConnectionString, pwd, testDatabaseName)
	db, err := pgx.Connect(context.Background(), testDbConnectionString)
	if err != nil {
		t.Errorf("runTestInAnotherDatabase() error: %v", err)
	}
	defer func(db *pgx.Conn, ctx context.Context) {
		err := db.Close(ctx)
		if err != nil {
			panic(err)
		}
	}(db, context.Background())

	// Create a new table in the test database
	createTableQuery := `
			CREATE TABLE test_table (
				id BIGINT PRIMARY KEY,
				name VARCHAR(1000) NOT NULL
			);`
	_, err = db.Exec(context.Background(), createTableQuery)
	if err != nil {
		t.Errorf("Failed to create table in test database '%s': %v", testDatabaseName, err)
		return
	}
	t.Logf("Table 'test_table' created successfully in database '%s'", testDatabaseName)

	mapper := FieldMapper{
		Info: ParquetFileInfo{
			TableName: "test_table",
			FileName:  "test_table.parquet",
			Columns: []ColumnInfo{
				{
					ColumnName:   "id",
					OriginalType: "bigint",
				},
				{
					ColumnName:   "name",
					OriginalType: "character varying",
				},
			},
		},
		Config: &config.Config{
			IncludeTables: make(map[string]struct{}),
			ExcludeTables: make(map[string]struct{}),
		},
	}

	testData := TestCopyFromSource{
		data: []TestDataRow{
			{id: 72148587066687490, name: "Alice"},
			{id: 72148596839153665, name: "Bob"},
			{id: 72148675837231105, name: "Charlie"},
			{id: 72148675837231106, name: "Dilan"},
			{id: 72161148674375736, name: "Eve"},
		},
		index: -1,
	}

	var copied int64
	copied, err = db.CopyFrom(
		context.Background(),
		utils.CreatePgxIdentifier("test_table"),
		mapper.getFieldNames(), //[]string{"first_name", "last_name", "age"},
		&testData,              // pgx.CopyFromRows(rows),
	)

	if err != nil {
		t.Errorf("Failed to copy data into table 'test_table': %v", err)
		return
	}

	// Verify the number of rows copied matches the test data size
	if copied != int64(len(testData.data)) {
		t.Errorf("Number of rows copied (%d) does not match the test data size (%d)", copied, len(testData.data))
	} else {
		t.Logf("Successfully copied %d rows into 'test_table'", copied)
	}

	// Check the count of records in the `test_table`
	var count int
	err = db.QueryRow(context.Background(), "SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Errorf("Failed to count records in 'test_table': %v", err)
		return
	}

	// Verify the count matches the inserted test data size
	if count != len(testData.data) {
		t.Errorf("Record count in 'test_table' (%d) does not match the expected test data size (%d)", count, len(testData.data))
	} else {
		t.Logf("Record count in 'test_table' is correct: %d", count)
	}
}

type TestDataRow struct {
	id   int64
	name string
}

type TestCopyFromSource struct {
	data  []TestDataRow
	index int
	err   error
}

func (t *TestCopyFromSource) Next() bool {
	t.index++
	return t.index < len(t.data)
}

func (t *TestCopyFromSource) Values() ([]any, error) {
	if t.index >= len(t.data) {
		t.err = io.EOF
		return nil, t.err
	}
	data := t.data[t.index]
	return []any{data.id, data.name}, nil
}

func (t *TestCopyFromSource) Err() error {
	return t.err
}
