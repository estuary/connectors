package connector

import (
	"fmt"
)

// streamV2Range is a key-hash subrange, as a channel name and a committed offset
// token report it.
type streamV2Range struct {
	keyBegin, keyEnd uint32
}

func (r streamV2Range) String() string {
	return fmt.Sprintf("[%08x, %08x]", r.keyBegin, r.keyEnd)
}
