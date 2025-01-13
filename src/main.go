package main

import (
	"context"
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

func main() {
	// reading configuration shall be the very first action because it also configures the logger
	conf := GetConfig()
	logger.Info("Starting the application")

	var source Source
	if conf.LocalDir != "" {
		logger.Info("Using local directory: ", zap.String("dir", conf.LocalDir))
		source = NewLocalSource(conf.LocalDir)
	} else {
		logger.Info("Using AWS S3 bucket: ", zap.String("bucket", conf.AWSBucketPath))
		// Create a credential provider with static credentials.
		credentialsProvider := credentials.NewStaticCredentialsProvider("YOUR_ACCESS_KEY_ID",
			"YOUR_SECRET_ACCESS_KEY", "") // Last parameter is session token, usually empty

		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentialsProvider),
			config.WithRegion("YOUR_AWS_REGION")) // e.g., "us-east-1"
		if err != nil {
			logger.Fatal("failed to load configuration", zap.Error(err))
		}

		client := s3.NewFromConfig(cfg)

		// Example S3 operation (list buckets)
		output, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
		if err != nil {
			logger.Fatal("failed to list buckets", zap.Error(err))
		}

		fmt.Println("Buckets:")
		for _, bucket := range output.Buckets {
			fmt.Println(*bucket.Name)
		}
		logger.Error("ERROR: Not implemented yet")
		return
	}

	reader := NewSourceReader(conf, source)

	if conf.ListCommand {
		err := reader.listDatabases()
		if err != nil {
			logger.Error("ERROR: ", zap.Error(err))
		}
		return
	}

	writer := NewDatabaseWriter(conf.DBHost, conf.DBPort, conf.DBName, conf.DBUser, conf.DBPassword, conf.DBSSLMode)
	err := writer.connect()
	if err != nil {
		logger.Error("Error connecting to the database: ", zap.Error(err))
		return
	}
	defer func() {
		writer.close()
	}()

	startTime := time.Now() // Start measuring time
	tables, err := writer.getTablesOrdered()
	if err != nil {
		logger.Error("Error working with the database: ", zap.Error(err))
		return
	}
	logger.Info("Retrieved tables from the database", zap.Int("count", len(tables)),
		zap.Duration("time", time.Since(startTime)))

	parquetTables, err := reader.iterateOverTables(tables)
	if err != nil {
		logger.Error("ERROR: ", zap.Error(err))
		return
	}
	logger.Info("Parsed Parquet files", zap.Int("count", len(parquetTables)),
		zap.Duration("time", time.Since(startTime)))

}
