package blackbox

import (
	"fmt"
	"os"
)

func (c *Capture) log(a ...any) {
	c.Logger(a...)
}

var defaultLogger = func(args ...any) {
	fmt.Fprintln(os.Stderr, args...)
}
