package testutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldCleanup(t *testing.T) {
	now := time.Unix(1793810174, 0)
	suffix := "_flow_test_1793810174"
	old := "_flow_test_1793806000"

	for _, tc := range []struct {
		item string
		want bool
	}{
		{"flow_checkpoints_v1", false},
		{"FLOW_CHECKPOINTS_V1", false},
		{"customer_table", false},
		{"truncate_standard_ab12cd34" + suffix, true},
		{"TRUNCATE_STANDARD_AB12CD34_FLOW_TEST_1793810174", true},
		{"truncate_standard_ab12cd34" + old, true},
		{"TRUNCATE_STANDARD_AB12CD34_FLOW_TEST_1793806000", true},
		{"TRUNCATE_STANDARD_AB12CD34_FLOW_TEST_1793810000", false},
	} {
		got, reason := shouldCleanup(now, tc.item, suffix)
		require.Equal(t, tc.want, got, "%s: %s", tc.item, reason)
	}
}
