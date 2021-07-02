package parser

import (
    "sync"

	log "github.com/sirupsen/logrus"
)
// firstError is used to accumulate at most one error, which will be the first one that occurs.
type firstError struct {
	first error
	mutex sync.Mutex
}

// SetIfNil sets the first error, if it is not already set, and if e is not nil. If the first error
// has already been set, then e is logged and ignored.
func (c *firstError) SetIfNil(e error) {
	if e == nil {
		return
	}
	c.mutex.Lock()
	if c.first == nil {
		c.first = e
	} else {
		log.WithField("error", e).Info("ignoring subsequent error")
	}
	c.mutex.Unlock()
}

// First returns the first error, or nil if no non-nil error was provided to OnError.
func (c *firstError) First() error {
	c.mutex.Lock()
	var e = c.first
	c.mutex.Unlock()
	return e
}
