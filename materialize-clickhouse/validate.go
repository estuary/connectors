package main

import (
	"context"

	pm "github.com/estuary/flow/go/protocols/materialize"
)

func (d *driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return d.sqlDriver.Validate(ctx, req)
}
