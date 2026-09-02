// Package logger configures the process-wide slog logger and provides
// structured logging helpers for the broker engine.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// Init configures slog as the default logger. level is one of
// "debug", "info", "warn", "error"; format is "json" or "text".
// sink, when non-nil, receives a copy of every formatted log line.
func Init(level, format string) {
	InitWithSink(level, format, nil)
}

// InitWithSink behaves like Init but tees every record into sink.
func InitWithSink(level, format string, sink io.Writer) {
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
		handler = slog.NewTextHandler(slogOut(sink), &slog.HandlerOptions{Level: lvl})
	} else {
		handler = slog.NewJSONHandler(slogOut(sink), &slog.HandlerOptions{Level: lvl})
	}
	slog.SetDefault(slog.New(handler))
}

// slogOut wraps the destination writer: sink (if any) tees into the base writer.
func slogOut(sink io.Writer) io.Writer {
	if sink == nil {
		return os.Stdout
	}
	return io.MultiWriter(os.Stdout, sink)
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
