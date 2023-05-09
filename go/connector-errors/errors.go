package errors

import (
	"errors"
	"os"

	log "github.com/sirupsen/logrus"
)

// UserError wraps a source error with a user-facing message for the error string. The source error
// can be provided so that it can be logged separately from the user-facing message for diagnostic
// purposes.
type UserError struct {
	message string
	source  error
}

// NewUserError creates a UserError that will output message as the error string.
func NewUserError(source error, message string) *UserError {
	return &UserError{
		message: message,
		source:  source,
	}
}

func (e *UserError) Unwrap() error {
	return e.source
}

func (e *UserError) Error() string {
	return e.message
}

// Source returns the wrapped source error.
func (e *UserError) Source() error {
	return e.source
}

// LogFinalError logs the UserError as a structured final log if the error is a UserError. Otherwise
// the raw error message is written to stdout as a newline. In either case the process will exit
// with status 1.
func LogFinalError(err error) {
	var userError *UserError
	if errors.As(err, &userError) {
		// Log the structured information from the error if this was specifically a user-facing
		// error.
		log.WithFields(log.Fields{
			// TODO(whb): Provide additional fields with this log message for enhanced
			// presentation (markdown, etc.).
			"source": userError.Source(),
		}).Fatal(userError)
	}

	_, _ = os.Stderr.WriteString(err.Error())
	_, _ = os.Stderr.Write([]byte("\n"))
	os.Exit(1)
}
