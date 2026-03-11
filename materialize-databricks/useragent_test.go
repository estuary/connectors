package main

import (
	"fmt"
	"regexp"
	"testing"
)

func TestProductVersion(t *testing.T) {
	if ok, err := regexp.MatchString(`^\d+\.0\.0$`, productVersion); err != nil {
		t.Error(err)
	} else if !ok {
		t.Error(fmt.Errorf("productVersion %q does not match pattern DIGITS.0.0", productVersion))
	}
}
