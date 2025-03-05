package errors

import (
	"errors"
	"fmt"
	"os"
	"strings"

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

// TransparentError is used for exiting with a status code of 1 and not logging any additional error
// information. This can be used in cases where some other process will provide the final error
// message logging and the connector is acting as a proxy for it, such as parsing in filesource
// captures.
type TransparentError struct {
	source error
}

// NewUserError creates a Transparent error wrapping a source error, but will not produce any
// additional logging when processed by HandleFinalError.
func NewTransparentError(source error) *TransparentError {
	return &TransparentError{
		source: source,
	}
}

func (e *TransparentError) Error() string { return e.source.Error() }

// HandleFinalError performs special handling for final errors when the error type is one that is
// defined in this package. For other errors, the error is logged on a newline.
func HandleFinalError(err error) {
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

	var transparentError *TransparentError
	if errors.As(err, &transparentError) {
		// Exit without any additional logging.
		os.Exit(1)
	}

	_, _ = os.Stderr.WriteString(err.Error())
	_, _ = os.Stderr.Write([]byte("\n"))
	os.Exit(1)
}

// PrereqErr is a wrapper for recording accumulated errors during prerequisite checking and
// formatting them for user presentation.
type PrereqErr struct {
	errs []error
}

// Err adds an error to the accumulated list of errors.
func (e *PrereqErr) Err(err error) {
	e.errs = append(e.errs, err)
}

func (e *PrereqErr) Len() int {
	return len(e.errs)
}

func (e *PrereqErr) Unwrap() []error {
	return e.errs
}

func (e *PrereqErr) Error() string {
	var b = new(strings.Builder)
	fmt.Fprintf(b, "the connector cannot run due to the following error(s):")
	for _, err := range e.errs {
		b.WriteString("\n - ")
		b.WriteString(err.Error())
	}
	return b.String()
}
