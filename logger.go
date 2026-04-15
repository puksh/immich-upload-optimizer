package main

import (
	"log"

	"github.com/fatih/color"
)

var (
	red       = color.New(color.FgRed).SprintfFunc()
	magenta   = color.New(color.FgMagenta).SprintfFunc()
	yellow    = color.New(color.FgYellow).SprintfFunc()
	green     = color.New(color.FgGreen).SprintfFunc()
	greenBold = color.New(color.FgGreen, color.Bold).SprintfFunc()
	cyan      = color.New(color.FgCyan).SprintfFunc()
	white     = color.New(color.FgWhite).SprintfFunc()
)

type customLogger struct {
	logger    *log.Logger
	prefix    string
	errPrefix string
}

func newCustomLogger(baseLogger interface{}, additionalPrefix string) *customLogger {
	switch logger := baseLogger.(type) {
	case *log.Logger:
		return &customLogger{
			logger: logger,
			prefix: additionalPrefix,
		}
	case *customLogger:
		return &customLogger{
			logger: logger.logger,
			prefix: logger.prefix + additionalPrefix,
		}
	default:
		panic("unsupported logger type")
	}
}

func (cl *customLogger) Println(v ...interface{}) {
	cl.logger.Println(cl.prefix, v)
}

func (cl *customLogger) Printf(format string, v ...interface{}) {
	cl.logger.Printf(cl.prefix+format, v...)
}

func (cl *customLogger) Print(s string) {
	cl.logger.Print(cl.prefix + s)
}

func (cl *customLogger) SetErrPrefix(prefix string) {
	cl.errPrefix = prefix
}

func (cl *customLogger) HasErrPrefix() bool {
	return cl.errPrefix != ""
}

func (cl *customLogger) Error(err error, errorType string) bool {
	if err != nil {
		if errorType != "" {
			errorType = errorType + ": "
		}
		cl.logger.Print(cl.prefix + red(cl.errPrefix+": "+errorType+"%v", err))
		return true
	}
	return false
}
