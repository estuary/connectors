package main

import (
	"context"
	firebase "firebase.google.com/go"
	"fmt"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"google.golang.org/api/option"
)

func (driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	// Validate connection to Firestore
	sa := option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		return nil, err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var out []*pc.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.ValidateResponse_Binding{
			ResourcePath: []string{res.Path},
		})
	}

	return &pc.ValidateResponse{Bindings: out}, nil
}
