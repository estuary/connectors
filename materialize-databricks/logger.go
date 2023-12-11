package main

import (
	"context"
	"github.com/databricks/databricks-sdk-go/logger"
)

type NoOpLogger struct{}

func (*NoOpLogger) Enabled(ctx context.Context, level logger.Level) bool {
	return false
}

func (*NoOpLogger) Tracef(ctx context.Context, format string, v ...any) {}
func (*NoOpLogger) Debugf(ctx context.Context, format string, v ...any) {}
func (*NoOpLogger) Infof(ctx context.Context, format string, v ...any)  {}
func (*NoOpLogger) Warnf(ctx context.Context, format string, v ...any)  {}
func (*NoOpLogger) Errorf(ctx context.Context, format string, v ...any) {}
