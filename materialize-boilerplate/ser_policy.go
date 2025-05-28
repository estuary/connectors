package boilerplate

import pf "github.com/estuary/flow/go/protocols/flow"

var (
	// SerPolicyStd is a general, conservative serialization policy that has
	// been used by materializations historically that have limitations on how
	// long their materialized fields can be.
	SerPolicyStd = &pf.SerPolicy{
		StrTruncateAfter:       1 << 16, // 64 KiB
		NestedObjTruncateAfter: 1000,
		ArrayTruncateAfter:     1000,
	}

	// SerPolicyDisabled is a serialization policy that completely disables
	// truncation of strings, nested objects, and arrays.
	SerPolicyDisabled = &pf.SerPolicy{
		StrTruncateAfter:       0,
		NestedObjTruncateAfter: 0,
		ArrayTruncateAfter:     0,
	}
)
