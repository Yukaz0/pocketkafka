package logger

import (
	"io"
	"log/slog"
	"os"
)

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

func slogOut(sink io.Writer) io.Writer {
	if sink == nil {
		return os.Stdout
	}
	return io.MultiWriter(os.Stdout, sink)
}
