// Package logger configures the process-wide slog logger and provides
// structured logging helpers for the broker engine.
package logger

import (
	"context"
	"log/slog"
	"os"
)

// Init configures slog as the default logger. level is one of
// "debug", "info", "warn", "error"; format is "json" or "text".
func Init(level, format string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	var handler slog.Handler
	if format == "text" {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	}
	slog.SetDefault(slog.New(handler))
}

// LogProduce emits a structured "record appended" log line.
func LogProduce(ctx context.Context, topic string, partition int32, offset int64, bytes int) {
	slog.InfoContext(ctx, "record appended",
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
		slog.Int64("offset", offset),
		slog.Int("bytes", bytes),
	)
}
