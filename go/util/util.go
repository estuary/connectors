package util

import (
	"fmt"
	"strings"
)

// CheckEndpointSpaces checks for extraneous leading or trailing whitespace and returns an error if
// there are any. It should be used as part of validation that an endpoint URL/address is
// well-formed.
func CheckEndpointSpaces(fieldName string, ep string) error {
	if strings.TrimSpace(ep) != ep {
		return fmt.Errorf("'%s' must not contain leading or trailing spaces", fieldName)
	}
	return nil
}
