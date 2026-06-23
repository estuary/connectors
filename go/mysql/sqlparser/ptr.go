// Copyright 2019 The Vitess Authors.
// Licensed under the Apache License, Version 2.0 (see the LICENSE file in this
// directory).
//
// ptrOf is the inlined replacement for vitess.io/vitess/go/ptr.Of (v0.21.3),
// reimplemented by Estuary Technologies, Inc.

package sqlparser

// ptrOf returns a pointer to the given value. It is the inlined replacement for
// the upstream `vitess.io/vitess/go/ptr` package's `Of` helper, used by the
// grammar actions in sql.y to build pointer-valued AST fields.
func ptrOf[T any](x T) *T {
	return &x
}
