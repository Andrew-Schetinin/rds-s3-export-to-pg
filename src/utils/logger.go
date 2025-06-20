package utils

import (
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// CustomLogger is a logger type that embeds zap.Logger to provide logging functionalities with additional features.
type CustomLogger struct {
	zap.Logger // Embedding Logger (composition)
}

// defaultLogger is a pre-configured development logger using the zap library for structured logging.
var defaultLogger, _ = zap.NewDevelopment()

// Logger shared logger for the whole program
var Logger = CustomLogger{*defaultLogger}

const (
	// LogTrace we need a more detailed log level to make DEBUG logs not so verbose.
	// DEBUG logs work on the level of whole tables, and TRACE logs work on the row level.
	LogTrace zapcore.Level = -3
)

// Trace logs a message at trace level with optional structured fields.
func (l *CustomLogger) Trace(msg string, fields ...zap.Field) {
	l.Log(LogTrace, msg, fields...)
}

// init Clean the logger at the end
func init() {
	setupShutdownHook()
}

// setupShutdownHook ensures that the logger's buffer is flushed and resources are cleaned up
// before the application exits.
func setupShutdownHook() {
	defer func(logger *CustomLogger) {
		err := logger.Sync()
		if err != nil {
			// instead of fatal, we just log the error and continue
			log.Println("Expected error in unit tests while syncing the logger: ", err)
		}
	}(&Logger) // Flushes buffer, if any
}

// InitLogger initializes the global logger with given options for JSON formatting, development mode, and verbosity.
func InitLogger(json bool, dev bool, verbose bool, trace bool) {
	if json {
		if trace {
			config := zap.Config{
				Level:       zap.NewAtomicLevelAt(LogTrace),
				Development: false,
				Sampling: &zap.SamplingConfig{
					Initial:    100,
					Thereafter: 100,
				},
				Encoding: "json",
				EncoderConfig: zapcore.EncoderConfig{
					TimeKey:        "ts",
					LevelKey:       "level",
					NameKey:        "logger",
					CallerKey:      "caller",
					FunctionKey:    zapcore.OmitKey,
					MessageKey:     "msg",
					StacktraceKey:  "stacktrace",
					LineEnding:     zapcore.DefaultLineEnding,
					EncodeLevel:    TraceLevelEncoder,
					EncodeTime:     zapcore.EpochTimeEncoder,
					EncodeDuration: zapcore.SecondsDurationEncoder,
					EncodeCaller:   zapcore.ShortCallerEncoder,
				},
				OutputPaths:      []string{"stderr"},
				ErrorOutputPaths: []string{"stderr"},
			}
			defaultLogger, _ = config.Build()
		} else if verbose {
			defaultLogger, _ = zap.NewProduction(zap.IncreaseLevel(zap.DebugLevel))
		} else {
			defaultLogger, _ = zap.NewProduction()
		}
		Logger = CustomLogger{*defaultLogger}
	} else if dev {
		if trace {
			config := zap.Config{
				Level:       zap.NewAtomicLevelAt(LogTrace),
				Development: true,
				Encoding:    "console",
				EncoderConfig: zapcore.EncoderConfig{
					// Keys can be anything except the empty string.
					TimeKey:        "T",
					LevelKey:       "L",
					NameKey:        "N",
					CallerKey:      "C",
					FunctionKey:    zapcore.OmitKey,
					MessageKey:     "M",
					StacktraceKey:  "S",
					LineEnding:     zapcore.DefaultLineEnding,
					EncodeLevel:    TraceLevelEncoder,
					EncodeTime:     zapcore.ISO8601TimeEncoder,
					EncodeDuration: zapcore.StringDurationEncoder,
					EncodeCaller:   zapcore.ShortCallerEncoder,
				},
				OutputPaths:      []string{"stderr"},
				ErrorOutputPaths: []string{"stderr"},
			}
			defaultLogger, _ = config.Build()
		} else if verbose {
			defaultLogger, _ = zap.NewDevelopment()
		} else {
			defaultLogger, _ = zap.NewDevelopment(zap.IncreaseLevel(zap.InfoLevel))
		}
		Logger = CustomLogger{*defaultLogger}
	} else {
		// Disable timestamps by setting log flags to 0.
		// We use this logger for console error output.
		log.SetFlags(0)

		// constructs console-friendly output, not meant for development
		level := zap.NewAtomicLevelAt(zap.InfoLevel)
		if trace {
			level = zap.NewAtomicLevelAt(LogTrace)
		} else if verbose {
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

		defaultLogger = zap.New(core, zap.WithCaller(false), zap.AddStacktrace(zapcore.ErrorLevel))
		Logger = CustomLogger{*defaultLogger}
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
	} else if l == LogTrace {
		enc.AppendString("TRACE")
	}
}

// TraceLevelEncoder adds TRACE level serialization, otherwise it prints LEVEL(-3)
func TraceLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if l == LogTrace {
		enc.AppendString("TRACE")
	} else {
		enc.AppendString(l.CapitalString())
	}
}
