package common

import (
	"context"
	"log/slog"
	"path"
	"slices"

	"github.com/sirupsen/logrus"
)

// LogrusHandler adapts a logrus.Logger to the standard Go log/slog interface.
type LogrusHandler struct {
	logger    *logrus.Logger
	withAttrs []slog.Attr
	withGroup string
}

func NewLogrusHandler(logger *logrus.Logger) *LogrusHandler {
	return &LogrusHandler{logger: logger}
}

func (l *LogrusHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= logrusToSlogLevel(l.logger.GetLevel())
}

func (l *LogrusHandler) Handle(ctx context.Context, r slog.Record) error {
	var entry = logrus.NewEntry(l.logger)
	if !r.Time.IsZero() {
		entry = entry.WithTime(r.Time)
	}

	for _, attr := range l.withAttrs {
		if attr.Key != "" {
			entry = entry.WithField(attr.Key, attr.Value)
		}
	}
	for attr := range r.Attrs {
		if attr.Key != "" {
			entry = entry.WithField(attr.Key, attr.Value)
		}
	}

	entry.Log(slogToLogrusLevel(r.Level), r.Message)
	return nil
}

func (l *LogrusHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var withAttrs = slices.Clone(l.withAttrs)
	for _, attr := range attrs {
		withAttrs = append(withAttrs, slog.Attr{
			Key:   l.withGroup + "." + attr.Key,
			Value: attr.Value,
		})
	}
	return &LogrusHandler{
		logger:    l.logger,
		withAttrs: withAttrs,
		withGroup: l.withGroup,
	}
}

func (l *LogrusHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return l
	}
	return &LogrusHandler{
		logger:    l.logger,
		withAttrs: l.withAttrs,
		withGroup: path.Join(l.withGroup, name),
	}
}

func slogToLogrusLevel(level slog.Level) logrus.Level {
	switch {
	case level >= slog.LevelError+8:
		return logrus.PanicLevel
	case level >= slog.LevelError+4:
		return logrus.FatalLevel
	case level >= slog.LevelError:
		return logrus.ErrorLevel
	case level >= slog.LevelWarn:
		return logrus.WarnLevel
	case level >= slog.LevelInfo:
		return logrus.InfoLevel
	case level >= slog.LevelDebug:
		return logrus.DebugLevel
	default:
		return logrus.TraceLevel
	}
}

func logrusToSlogLevel(level logrus.Level) slog.Level {
	switch level {
	case logrus.PanicLevel:
		return slog.LevelError + 8
	case logrus.FatalLevel:
		return slog.LevelError + 4
	case logrus.ErrorLevel:
		return slog.LevelError
	case logrus.WarnLevel:
		return slog.LevelWarn
	case logrus.DebugLevel:
		return slog.LevelDebug
	case logrus.TraceLevel:
		return slog.LevelDebug - 4
	default:
		return slog.LevelInfo
	}
}
