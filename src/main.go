package main

import (
	"context"
	config2 "dbrestore/config"
	"dbrestore/utils"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"time"

	_ "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// log a convenience wrapper to shorten code lines
var log = utils.Logger

func main() {
	// reading configuration shall be the very first action because it also configures the logger
	conf := config2.GetConfig()
	log.Info("Starting the application")

	var source Source
	if conf.LocalDir != "" {
		log.Info("Using local directory: ", zap.String("dir", conf.LocalDir))
		source = NewLocalSource(conf.LocalDir)
	} else {
		log.Info("Using AWS S3 bucket: ", zap.String("bucket", conf.AWSBucketPath))
		// Create a credential provider with static credentials.
		credentialsProvider := credentials.NewStaticCredentialsProvider("YOUR_ACCESS_KEY_ID",
			"YOUR_SECRET_ACCESS_KEY", "") // Last parameter is session token, usually empty

		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentialsProvider),
			config.WithRegion("YOUR_AWS_REGION")) // e.g., "us-east-1"
		if err != nil {
			log.Fatal("failed to load configuration", zap.Error(err))
		}

		client := s3.NewFromConfig(cfg)

		// Example S3 operation (list buckets)
		output, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
		if err != nil {
			log.Fatal("failed to list buckets", zap.Error(err))
		}

		fmt.Println("Buckets:")
		for _, bucket := range output.Buckets {
			fmt.Println(*bucket.Name)
		}
		log.Error("ERROR: Not implemented yet")
		return
	}

	reader := NewSourceReader(conf, source)

	if conf.ListCommand {
		err := reader.listDatabases()
		if err != nil {
			log.Error("ERROR: ", zap.Error(err))
		}
		return
	}

	writer := NewDatabaseWriter(conf.DBHost, conf.DBPort, conf.DBName, conf.DBUser, conf.DBPassword, conf.DBSSLMode)
	err := writer.connect()
	if err != nil {
		log.Error("Error connecting to the database: ", zap.Error(err))
		return
	}
	defer func() {
		writer.close()
	}()

	// Get the list of tables from PostgreSQL database - we can only populate these tables.
	// The order is calculated based on relations between tables and it is very important.
	startTime := time.Now()
	tables, err := writer.getTablesOrdered()
	if err != nil {
		log.Error("Error working with the database: ", zap.Error(err))
		return
	}
	log.Info("Retrieved tables from the database", zap.Int("count", len(tables)),
		zap.Duration("time", time.Since(startTime)))

	if conf.TruncateAllCommand {
		startTime2 := time.Now()
		truncatedCount, err := writer.truncateAllTables(tables)
		if err != nil {
			log.Error("Error truncating tables: ", zap.Error(err))
			return
		}
		log.Info("Truncating all tables done", zap.Int("truncatedCount", truncatedCount),
			zap.Duration("time", time.Since(startTime2)))
	}

	// Get the list of tables in Parquet files - we only have data for those tables
	parquetTables, err := reader.iterateOverTables(tables)
	if err != nil {
		log.Error("ERROR: ", zap.Error(err))
		return
	}
	log.Info("Parsed Parquet files", zap.Int("count", len(parquetTables)),
		zap.Duration("time", time.Since(startTime)))

	// Convert parquetTables list to a map where the table name is the key
	parquetTableMap := make(map[string]ParquetFileInfo)
	for _, table := range parquetTables {
		parquetTableMap[table.TableName] = table
	}

	// Iterate over the list of tables in the correct order and process them
	for _, table := range tables {
		if parquetInfo, exists := parquetTableMap[table]; exists {
			// Construct the field mapper that defines the strategy of loading this table
			mapper, err := writer.getFieldMapper(parquetInfo, conf)
			if err != nil {
				log.Error("Error mapping fields for table", zap.String("table", table), zap.Error(err))
				continue
			}

			if reason, skip := mapper.shouldSkip(); skip {
				log.Info("Skipping table", zap.String("table", table), zap.String("reason", reason))
			} else {
				// Write data to the corresponding database table
				tableStartTime := time.Now()
				recordCount, err := writer.writeTable(source, &mapper)
				if err != nil {
					log.Error("Error writing data for table", zap.String("table", table), zap.Error(err))
					break
				}
				duration := time.Since(tableStartTime)
				recordsPerSecond := 0.0
				if duration.Seconds() > 0 {
					recordsPerSecond = float64(recordCount) / duration.Seconds()
				} else if duration.Microseconds() > 0 {
					recordsPerSecond = (float64(recordCount) * 1000000.0) / float64(duration.Microseconds())
				}
				log.Info("Loaded table data", zap.String("table", table),
					zap.Int("records", recordCount), zap.Duration("time", duration),
					zap.Float64("records/sec", recordsPerSecond))
			}
		}
	}
	log.Info("Finished processing all tables", zap.Duration("total_time", time.Since(startTime)))
}
