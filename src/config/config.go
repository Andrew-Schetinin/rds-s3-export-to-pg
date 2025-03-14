package config

import (
	"context"
	"dbrestore/utils"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// Config represents the application configuration defined through various sources
// such as environment variables or files.
type Config struct {

	// ListCommand list database instances (subfolders) in the exported database cluster and exit
	ListCommand bool

	// TruncateAllCommand indicates whether all tables in the destination database should be truncated before loading data.
	TruncateAllCommand bool

	// SourceDatabase specifies the database name from the local folder or S3 bucket to be restored;
	// it can be skipped if there is only one database instance in the exported snapshot
	SourceDatabase string

	// IncludeTables specifies a comma-separated list of table names to be included in the operation
	// (with or without schema names).
	IncludeTables map[string]struct{}

	// ExcludeTables specifies a comma-separated list of table names to be excluded from the operation
	// (with or without schema names).
	ExcludeTables map[string]struct{}

	// IgnoreMissingTablePrefixes specifies a set of table name prefixes to be ignored if missing
	// in the destination database (with or without schema names); this can be useful in cases of partitioned tables.
	IgnoreMissingTablePrefixes map[string]struct{}

	// SkipNotEmpty skips all tables that are not empty in the target database - it allows loading data incrementally.
	// Note that it may cause data loss if there are multiple Parquet files and some failed to load.
	SkipNotEmpty bool

	// LocalDir specifies the localPath to the local directory containing Parquet files, used if no S3 bucket is provided.
	LocalDir string

	// AWSBucketPath specifies the complete ARN of the AWS S3 bucket used for storing or retrieving Parquet files
	// and the localPath to the exported snapshot. Used if no local directory is provided.
	AWSBucketPath string

	// AWSAccessKey specifies the AWS access key used for authentication with AWS services.
	AWSAccessKey string

	// AWSSecretKey represents the secret key used for AWS authentication.
	AWSSecretKey string

	// AWSRegion specifies the AWS region for connecting to S3.
	AWSRegion string

	// DBHost specifies the hostname or IP address of the database server to connect to.
	DBHost string

	// DBPort specifies the port number used to connect to the database server.
	DBPort int

	// DBName specifies the name of the database to connect to - this is the destination database.
	DBName string

	// DBUser specifies the username for database authentication.
	DBUser string

	// DBPassword holds the database password for authentication.
	DBPassword string

	// DBSSLMode specifies whether SSL mode is enabled for database connections.
	DBSSLMode bool

	// AWSConfig AWS configuration in case we load it from a configuration file.
	// we should not use complex types because reflection will stop working - pointers are okay
	AWSConfig *aws.Config
}

// Singleton initialization - it is lazy-loaded and thread-safe
var (
	// instance the actual configuration after checking all possible configuration sources
	instance *Config
	once     sync.Once
)

// GetConfig initializes and returns a singleton instance of the Config struct with values loaded from various sources.
// Command line arguments override all other configuration sources.
func GetConfig() *Config {
	once.Do(func() {
		// first read the command line arguments because they can affect the rest of the initialization
		var argsInstance = &Config{}
		argsInstance.loadFromArguments()
		// now initialize the configuration
		instance = &Config{}
		// Load configuration from various sources (in order of precedence)
		instance.loadFromEnv()
		instance.loadFromFile() // Example of loading from a config file
		instance.loadAWSConfig()
		instance.override(argsInstance) // some arguments can override other configuration sources
		instance.validate()
	})
	return instance
}

// loadFromEnv loads configuration values from environment variables and assigns them to the Config struct fields.
func (c *Config) loadFromEnv() {
	// Load from environment variables
	if region := os.Getenv("AWS_REGION"); region != "" {
		c.AWSRegion = region
	}
	//if bucketName := os.Getenv("S3_BUCKET_NAME"); bucketName != "" {
	//	c.AWSBucketName = bucketName
	//}
	// ... load other parameters
}

// loadFromFile loads configuration data from a file (e.g., JSON, YAML) and populates the Config struct.
// TODO: not implemented yet
func (c *Config) loadFromFile() {
	// Load from a config file (e.g., JSON, YAML)
	// You would use a library like "encoding/json" or "gopkg.in/yaml.v3" here.
	// Example (using a placeholder):
	// if fileExists("config.json") {
	//      // load config.json and populate config
	// }
}

// loadAWSConfig loads AWS configuration using the AWS SDK, applying region from Config and environment variable overrides.
// It initializes the AWSConfig field and logs a fatal error if loading fails.
// TODO: not implemented yet
func (c *Config) loadAWSConfig() {
	// Load AWS config, allowing environment variables and shared config to override
	var awsConfig aws.Config
	awsConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(c.AWSRegion))
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %v", err)
	}
	c.AWSConfig = &awsConfig
}

// validate Perform validation of required parameters
func (c *Config) validate() {
	if c.LocalDir == "" && c.AWSBucketPath == "" {
		log.Fatal("Error: RDS export local path or remote bucket is required.\n" +
			"Run with --help for more information.")
	}
	if !c.ListCommand && c.DBName == "" {
		log.Fatal("Error: Database name is required.\n" +
			"Run with --help for more information.")
	}
}

// loadFromArguments Define command-line flags
func (c *Config) loadFromArguments() {
	helpCommand := flag.Bool("help", false, "Get help on how to use the application")

	// First we define the structure of the command line arguments - before actually parsing them.
	// Don't try to initialize any configurations here because it will not work before flag.Parse()
	jsonLogs := flag.Bool("json-logs", false,
		"Enable production JSON-formatted logs")
	verboseLogs := flag.Bool("verbose", false,
		"Enable verbose DEBUG-level logging")
	traceLogs := flag.Bool("trace", false,
		"Enable even more verbose TRACE-level logging")
	developmentLogs := flag.Bool("dev-logs", false,
		"Enable development logs formatting with time stamps and source files")

	listCommand := flag.Bool("list", false,
		"List database instances (subfolders) in the exported database cluster and exit")

	truncateAllCommand := flag.Bool("truncate-all", false,
		"Truncate all tables in the destination database before loading the data")

	sourceDatabase := flag.String("source-db", "",
		"The database name from the local folder or S3 bucket to be restored. "+
			"It can be skipped if there is only one database instance in the exported snapshot.")

	localDir := flag.String("dir", "",
		"Local directory with the Parquet files (optional, required if --s3-bucket is not specified)")

	includeTables := flag.String("include-tables", "",
		"specifies a comma-separated list of table names to be included in the operation (with or without schema names)")
	excludeTables := flag.String("exclude-tables", "",
		"specifies a comma-separated list of table names to be excluded from the operation (with or without schema names)")

	ignoreMissingTablePrefixes := flag.String("ignore-missing-tables", "",
		"specifies a comma-separated list of table name prefixes to be ignored if missing "+
			"in the destination database (with or without schema names); this can be useful in cases of partitioned tables")
	SkipNotEmpty := flag.Bool("skip-not-empty", false,
		"skips all tables that are not empty in the target database - it allows loading data incrementally; "+
			"note that it may cause data loss if there are multiple Parquet files and some failed to load.")

	awsAccessKey := flag.String("aws-access-key", "", "AWS Access Key (required when using S3 bucket)")
	awsSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key (required when using S3 bucket)")
	awsRegion := flag.String("aws-region", "", "AWS Region (required when using S3 bucket)")

	//parquetFile := flag.String("parquet-file", "", "Path to the Parquet file to process (required)")

	dbUser := flag.String("db-user", "", "Database username")
	dbPassword := flag.String("db-password", "", "Database password")
	dbHost := flag.String("db-host", "localhost", "Database host")
	dbPort := flag.String("db-port", "5432", "Database port")
	dbName := flag.String("db-name", "", "Database name")
	//dbSSLMode := flag.String("db-sslmode", "disable", "Database SSL mode (default: 'disable')")

	// Parse the flags
	flag.Parse()

	// the logger initialization should happen first of all
	utils.InitLogger(jsonLogs != nil && *jsonLogs, developmentLogs != nil && *developmentLogs,
		verboseLogs != nil && *verboseLogs, traceLogs != nil && *traceLogs)

	flag.Usage = func() {
		_, err := fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		if err != nil {
			return
		}
		flag.PrintDefaults()
	}

	if helpCommand != nil && *helpCommand {
		flag.Usage()
		os.Exit(0)
	}

	// only now we can actually read the command line arguments and use them
	if listCommand != nil && *listCommand {
		c.ListCommand = true
	}
	if truncateAllCommand != nil && *truncateAllCommand {
		c.TruncateAllCommand = true
	}
	if SkipNotEmpty != nil && *SkipNotEmpty {
		c.SkipNotEmpty = true
	}
	if isNotBlank(sourceDatabase) {
		c.SourceDatabase = *sourceDatabase
	}
	if isNotBlank(localDir) {
		c.LocalDir = *localDir
	}
	c.IncludeTables = createSet(includeTables)
	c.ExcludeTables = createSet(excludeTables)
	c.IgnoreMissingTablePrefixes = createSet(ignoreMissingTablePrefixes)
	if isNotBlank(awsAccessKey) {
		c.AWSAccessKey = *awsAccessKey
	}
	if isNotBlank(awsSecretKey) {
		c.AWSSecretKey = *awsSecretKey
	}
	if isNotBlank(awsRegion) {
		c.AWSRegion = *awsRegion
	}
	if isNotBlank(dbUser) {
		c.DBUser = *dbUser
	}
	if isNotBlank(dbPassword) {
		c.DBPassword = *dbPassword
	}
	if isNotBlank(dbHost) {
		c.DBHost = *dbHost
	}
	if isNotBlank(dbPort) {
		if isNotBlank(dbPort) {
			port, err := strconv.Atoi(*dbPort)
			if err != nil {
				log.Fatalf("invalid value for db-port: %v", err)
			}
			c.DBPort = port
		}
	}
	if isNotBlank(dbName) {
		c.DBName = *dbName
	}
}

// override updates the current Config instance's fields by overriding them with non-zero values
// from another Config instance.
func (c *Config) override(argsInstance *Config) {
	v := reflect.ValueOf(argsInstance).Elem()
	t := reflect.TypeOf(argsInstance).Elem()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Get the corresponding field in the original 'c' structure
		cField := reflect.ValueOf(c).Elem().FieldByName(fieldType.Name)

		// Check if the field exists and is settable
		if cField.IsValid() && cField.CanSet() {
			switch field.Kind() {
			case reflect.String:
				if field.String() != "" {
					cField.Set(field)
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if field.Int() != 0 {
					cField.Set(field)
				}
			case reflect.Map, reflect.Slice:
				if !field.IsNil() {
					cField.Set(field)
				}
			case reflect.Bool:
				if field.Bool() {
					cField.Set(field)
				}
			case reflect.Ptr:
				if !field.IsNil() {
					cField.Set(field)
				}
			default:
				panic("unhandled default case")
			}
		}
	}
}

// TableNameInSet checks if a given table name exists in the provided set and determines if the set is non-empty.
// Both the input fullTableName and the configuration tables set can contain optional schema names.
// In order to be found, the table name must fully match, while schema name is optional -
// it must only match if both schemas are specified.
func (c *Config) TableNameInSet(tables map[string]struct{}, fullTableName string) (found bool, notEmpty bool) {
	notEmpty = len(tables) > 0
	found = false
	if notEmpty {
		schema, table := utils.SplitFullTableName(fullTableName)
		for testFullTableName := range tables {
			configSchema, configTable := utils.SplitFullTableName(testFullTableName)
			// table name must fully match, while schema name is optional - it must only match if both schemas are specified
			if configTable == table && (configSchema == schema || schema == "" || configSchema == "") {
				found = true
				break
			}
		}
	}
	return
}

// isNotBlank checks if the provided string pointer is non-nil and its trimmed value is not empty.
func isNotBlank(s *string) bool {
	return s != nil && strings.TrimSpace(*s) != ""
}

// createSet converts a comma-separated string into a set of strings, returning a map with unique keys as set elements.
func createSet(s *string) map[string]struct{} {
	ret := make(map[string]struct{})
	if isNotBlank(s) {
		for _, prefix := range strings.Split(*s, ",") {
			ret[strings.TrimSpace(prefix)] = struct{}{}
		}
	}
	return ret
}
