package main

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
)

// logger shared logger for the whole program
var logger, _ = zap.NewDevelopment() // the default logger

// init Clean the logger at the end
func init() {
	setupShutdownHook()
}

// setupShutdownHook ensures that the logger's buffer is flushed and resources are cleaned up
// before the application exits.
func setupShutdownHook() {
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			log.Fatal("ERROR syncing the logger: ", err)
		}
	}(logger) // Flushes buffer, if any
}

// initLogger initializes the global logger with given options for JSON formatting, development mode, and verbosity.
func initLogger(json bool, dev bool, verbose bool) {
	if json {
		if verbose {
			logger, _ = zap.NewProduction(zap.IncreaseLevel(zap.DebugLevel))
		} else {
			logger, _ = zap.NewProduction()
		}
	} else if dev {
		if verbose {
			logger, _ = zap.NewDevelopment()
		} else {
			logger, _ = zap.NewDevelopment(zap.IncreaseLevel(zap.InfoLevel))
		}
	} else {
		// Disable timestamps by setting log flags to 0.
		// We use this logger for console error output.
		log.SetFlags(0)

		// constructs console-friendly output, not meant for development
		level := zap.NewAtomicLevelAt(zap.InfoLevel)
		if verbose {
			level = zap.NewAtomicLevelAt(zap.DebugLevel)
		}

		encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
			MessageKey:     "message",                     // Set the key for the log message
			LevelKey:       "level",                       // Leave blank to omit the log level
			TimeKey:        "",                            // Leave blank to omit the timestamp
			CallerKey:      "caller",                      // Key for caller information (optional)
			EncodeLevel:    IconLevelEncoder,              // instead of zapcore.CapitalLevelEncoder
			EncodeCaller:   zapcore.ShortCallerEncoder,    // Optional: Include short caller info
			EncodeDuration: zapcore.StringDurationEncoder, // Format for durations
		})

		writer := zapcore.AddSync(os.Stdout) // &CustomWriter{})

		core := zapcore.NewCore(
			encoder, // Encoder
			writer,  //customSyncer, // Custom write syncer
			level,   // Log everything from DEBUG and above
		)

		logger = zap.New(core, zap.WithCaller(false), zap.AddStacktrace(zapcore.ErrorLevel))
	}
	setupShutdownHook()
}

// IconLevelEncoder serializes a Level to an icon - only for more important levels.
func IconLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if l == zapcore.ErrorLevel || l == zapcore.FatalLevel { // Check if it's an error message
		enc.AppendString("❌") // Prepend the symbol to the message
	} else if l == zapcore.WarnLevel {
		enc.AppendString("⚠️") // Prepend the symbol to the message
	} else if l == zapcore.InfoLevel {
		enc.AppendString("ℹ️") // Prepend the symbol to the message
	}
}
