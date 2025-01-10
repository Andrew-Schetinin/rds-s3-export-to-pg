package main

import (
	"context"
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

	// IncludeTables specifies a comma-separated list of table names to be included in the operation
	// (with or without schema names).
	IncludeTables string

	// ExcludeTables specifies a comma-separated list of table names to be excluded from the operation
	// (with or without schema names).
	ExcludeTables string

	// IgnoreMissingTablePrefixes specifies a set of table name prefixes to be ignored if missing
	// in the destination database (with or without schema names); this can be useful in cases of partitioned tables.
	IgnoreMissingTablePrefixes map[string]struct{}

	// LocalDir specifies the localPath to the local directory containing Parquet files, used if no S3 bucket is provided.
	LocalDir string

	// AWSBucketPath specifies the complete ARN of the AWS S3 bucket used for storing or retrieving Parquet files
	// and the localPath to the exported snapshot. Used if no local directory is provided.
	AWSBucketPath string

	AWSAccessKey string
	AWSSecretKey string
	AWSRegion    string

	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string
	DBSSLMode  bool

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

func (c *Config) loadFromFile() {
	// Load from a config file (e.g., JSON, YAML)
	// You would use a library like "encoding/json" or "gopkg.in/yaml.v3" here.
	// Example (using a placeholder):
	// if fileExists("config.json") {
	//      // load config.json and populate config
	// }
}

func (c *Config) loadAWSConfig() {
	// Load AWS config, allowing environment variables and shared config to override
	var awsConfig aws.Config
	awsConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(c.AWSRegion))
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %v", err)
	}
	c.AWSConfig = &awsConfig
}

func (c *Config) validate() {
	// Perform validation of required parameters
	if c.LocalDir == "" && c.AWSRegion == "" {
		log.Fatal("AWS_REGION is required")
	}
	//if c.AWSBucketName == "" {
	//	log.Fatal("S3_BUCKET_NAME is required")
	//}
	// ... validate other parameters
}

// loadFromArguments Define command-line flags
func (c *Config) loadFromArguments() {
	// First we define the structure of the command line arguments - before actually parsing them.
	// Don't try to initialize any configurations here because it will not work before flag.Parse()
	jsonLogs := flag.Bool("json-logs", false,
		"Enable production JSON-formatted logs (false by default)")
	verboseLogs := flag.Bool("verbose", false,
		"Enable verbose DEBUG-level logging (false by default)")
	developmentLogs := flag.Bool("dev-logs", false,
		"Enable development logs formatting with time stamps and source files (false by default)")

	listCommand := flag.Bool("list", false,
		"List database instances (subfolders) in the exported database cluster and exit")

	localDir := flag.String("dir", "",
		"Local directory with the Parquet files (optional, required if --s3-bucket is not specified)")

	includeTables := flag.String("include-tables", "",
		"specifies a comma-separated list of table names to be included in the operation (with or without schema names)")
	excludeTables := flag.String("exclude-tables", "",
		"specifies a comma-separated list of table names to be excluded from the operation (with or without schema names)")

	ignoreMissingTablePrefixes := flag.String("ignore-missing-tables", "",
		"specifies a comma-separated list of table name prefixes to be ignored if missing "+
			"in the destination database (with or without schema names); this can be useful in cases of partitioned tables")

	awsAccessKey := flag.String("aws-access-key", "", "AWS Access Key (required)")
	awsSecretKey := flag.String("aws-secret-key", "", "AWS Secret Key (required)")
	awsRegion := flag.String("aws-region", "us-east-1", "AWS Region (default: 'us-east-1')")

	//parquetFile := flag.String("parquet-file", "", "Path to the Parquet file to process (required)")

	dbUser := flag.String("db-user", "", "Database username (default: 'andrews')")
	dbPassword := flag.String("db-password", "", "Database password")
	dbHost := flag.String("db-host", "localhost", "Database host (default: 'localhost')")
	dbPort := flag.String("db-port", "5432", "Database port (default: '5432')")
	dbName := flag.String("db-name", "", "Database name (default: 'tms_test')")
	//dbSSLMode := flag.String("db-sslmode", "disable", "Database SSL mode (default: 'disable')")

	// Parse the flags
	flag.Parse()

	// the logger initialization should happen first of all
	initLogger(jsonLogs != nil && *jsonLogs, developmentLogs != nil && *developmentLogs,
		verboseLogs != nil && *verboseLogs)

	flag.Usage = func() {
		_, err := fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		if err != nil {
			return
		}
		flag.PrintDefaults()
	}

	// only now we can actually read the command line arguments and use them
	if listCommand != nil && *listCommand {
		c.ListCommand = true
	}

	if isNotBlank(localDir) {
		c.LocalDir = *localDir
	}
	if isNotBlank(includeTables) {
		c.IncludeTables = *includeTables
	}
	if isNotBlank(excludeTables) {
		c.ExcludeTables = *excludeTables
	}
	c.IgnoreMissingTablePrefixes = make(map[string]struct{})
	if isNotBlank(ignoreMissingTablePrefixes) {
		for _, prefix := range strings.Split(*ignoreMissingTablePrefixes, ",") {
			c.IgnoreMissingTablePrefixes[strings.TrimSpace(prefix)] = struct{}{}
		}
	}
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

// isNotBlank checks if the provided string pointer is non-nil and its trimmed value is not empty.
func isNotBlank(s *string) bool {
	return s != nil && strings.TrimSpace(*s) != ""
}
