package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

type config struct {
	CredentialsJSON string `json:"googleCredentials"`
	SpreadsheetURL  string `json:"spreadsheetUrl"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if c.CredentialsJSON == "" {
		return fmt.Errorf("missing required Google service account credentials")
	} else if _, err := parseSheetsID(c.SpreadsheetURL); err != nil {
		return err
	}
	return nil
}

func (c config) spreadsheetID() string {
	var id, err = parseSheetsID(c.SpreadsheetURL)
	if err != nil {
		panic(err)
	}
	return id
}

func (c config) buildService() (*sheets.Service, error) {
	var client, err = sheets.NewService(context.Background(),
		option.WithCredentialsJSON([]byte(c.CredentialsJSON)))
	if err != nil {
		return nil, fmt.Errorf("building sheets service: %w", err)
	}
	return client, nil
}

type resource struct {
	Sheet string `json:"sheet"`
}

func (r resource) Validate() error {
	if r.Sheet == "" {
		return fmt.Errorf("missing required sheet name")
	}
	return nil
}

type driverCheckpoint struct {
	Round int64
}

func (c driverCheckpoint) Validate() error {
	return nil
}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "Google Sheets Materialization",
		"type":    "object",
		"required": [
			"googleCredentials",
			"spreadsheetUrl"
		],
		"properties": {
			"googleCredentials": {
				"type":        "string",
				"title":       "Google Service Account",
				"description": "Service account JSON key to use as Application Default Credentials",
				"multiline":   true,
				"secret":      true
			},
			"spreadsheetUrl": {
				"type":        "string",
				"title":       "Spreadsheet URL",
				"description": "URL of the spreadsheet to materialize into, which is shared with the service account."
			}
		}
		}`),
		ResourceSpecSchemaJson: json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "Google Sheets Materialization Binding",
		"type":    "object",
		"required": [
			"sheet"
		],
		"properties": {
			"sheet": {
				"type":        "string",
				"title":       "Sheet Name",
				"description": "Name of the spreadsheet sheet to materialize into"
			}
		}
		}`),
		DocumentationUrl: "https://go.estuary.dev/materialize-google-sheets",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, err
	}

	var svc, err = cfg.buildService()
	if err != nil {
		return nil, err
	} else if _, err = loadSheetIDMapping(svc, cfg.spreadsheetID()); err != nil {
		return nil, fmt.Errorf("verifying credentials: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {

		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		var constraints = make(map[string]*pm.Constraint)
		for _, projection := range binding.Collection.Projections {
			var constraint = new(pm.Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			case projection.IsPrimaryKey:
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "Components of the collection key must be materialized"
			case strings.HasPrefix(projection.Field, "_meta"):
				constraint.Type = pm.Constraint_FIELD_OPTIONAL
				constraint.Reason = "Metadata fields are optional"
			case projection.Inference.IsSingleScalarType():
				constraint.Type = pm.Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "Scalar types are recommended for materialization"
			default:
				constraint.Type = pm.Constraint_FIELD_OPTIONAL
				constraint.Reason = "Field is optional"
			}
			constraints[projection.Field] = constraint
		}

		out = append(out, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			DeltaUpdates: false,
			ResourcePath: []string{res.Sheet},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return d.apply(ctx, req, false)
}

func (d driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return d.apply(ctx, req, true)
}

func (driver) apply(ctx context.Context, req *pm.ApplyRequest, isDelete bool) (*pm.ApplyResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, err
	}

	var svc, err = cfg.buildService()
	if err != nil {
		return nil, err
	}
	sheetIDs, err := loadSheetIDMapping(svc, cfg.spreadsheetID())
	if err != nil {
		return nil, err
	}

	var actions []*sheets.Request
	var description string
	var rand = rand.New(rand.NewSource(time.Now().UnixMicro()))

	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var _, exists = sheetIDs[res.Sheet]

		if !exists && !isDelete {
			description += fmt.Sprintf("Created sheet %q.\n", res.Sheet)

			// Create a new sheet.
			var sheetID = int64(rand.Int31())
			actions = append(actions, &sheets.Request{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						Title:   res.Sheet,
						SheetId: sheetID,
					},
				},
			})
		} else if exists && isDelete {
			description += fmt.Sprintf("Deleted sheet %q.\n", res.Sheet)

			actions = append(actions, &sheets.Request{
				DeleteSheet: &sheets.DeleteSheetRequest{
					SheetId: sheetIDs[res.Sheet],
				},
			})
		}
	}

	if req.DryRun || len(actions) == 0 {
		// Nothing to do.
	} else if err = batchRequestWithRetry(ctx, svc, cfg.spreadsheetID(), actions); err != nil {
		return nil, fmt.Errorf("while updated sheets: %w", err)
	}

	return &pm.ApplyResponse{ActionDescription: description}, nil
}

// Transactions implements the DriverServer interface.
func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	var cfg config
	if err = pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint driverCheckpoint
	if err = pf.UnmarshalStrict(open.Open.DriverCheckpointJson, &checkpoint); err != nil {
		return fmt.Errorf("parsing driver checkpoint: %w", err)
	}

	svc, err := cfg.buildService()
	if err != nil {
		return err
	}

	states, err := loadSheetStates(
		open.Open.Materialization.Bindings,
		svc,
		cfg.spreadsheetID(),
	)
	if err != nil {
		return fmt.Errorf("recovering sheet states: %w", err)
	}

	if err := writeSheetSentinels(stream.Context(), svc, cfg.spreadsheetID(), states); err != nil {
		return fmt.Errorf("writing sheet sentinels: %w", err)
	}

	bindings, err := buildTransactorBindings(
		open.Open.Materialization.Bindings,
		checkpoint.Round,
		states,
	)
	if err != nil {
		return err
	}

	if err := writeSheetHeaders(stream.Context(), svc, cfg.spreadsheetID(), bindings); err != nil {
		return fmt.Errorf("writing sheet headers: %w", err)
	}

	var transactor = &transactor{
		bindings:      bindings,
		client:        svc,
		round:         checkpoint.Round,
		spreadsheetId: cfg.spreadsheetID(),
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: nil},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return pm.RunTransactions(stream, transactor, logrus.NewEntry(logrus.StandardLogger()))
}

func main() { boilerplate.RunMain(new(driver)) }
