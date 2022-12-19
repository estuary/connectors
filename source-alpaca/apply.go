package main

import (
	"context"

	pc "github.com/estuary/flow/go/protocols/capture"
)

func (driver) apply(ctx context.Context, req *pc.ApplyRequest, isDelete bool) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{
		ActionDescription: "",
	}, nil
}

func (d driver) ApplyUpsert(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return d.apply(ctx, req, false)
}

func (d driver) ApplyDelete(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return d.apply(ctx, req, true)
}
