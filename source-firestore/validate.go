package main

import (
	"context"
	"fmt"

	firestore "cloud.google.com/go/firestore"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"google.golang.org/api/option"
)

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	// Validate connection to Firestore
	credsOpt := option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))

	_, projectID, databaseID, err := resolveDatabasePath(ctx, cfg.DatabasePath, credsOpt)
	if err != nil {
		return nil, err
	}

	client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID, credsOpt)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Path},
		})
	}

	return &pc.Response_Validated{Bindings: out}, nil
}
