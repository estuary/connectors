package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-google-sheets/sheets_util"
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
	SpreadsheetID   string `json:"spreadsheetId"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if c.CredentialsJSON == "" {
		return fmt.Errorf("missing required Google service account credentials")
	}
	if c.SpreadsheetID == "" {
		return fmt.Errorf("missing required spreadsheet ID")
	}
	return nil
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
			"spreadsheetId"
		],
		"properties": {
			"googleCredentials": {
				"type":        "string",
				"title":       "Google Service Account",
				"description": "Service account JSON key to use as Application Default Credentials",
				"multiline":   true,
				"secret":      true
			},
			"spreadsheetId": {
				"type":        "string",
				"title":       "Spreadsheet ID",
				"description": "ID of the spreadsheet to materialize, which is shared with the service account."
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
		DocumentationUrl: "https://docs.estuary.dev",
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
	} else if _, err = sheets_util.LoadSheetIDMapping(svc, cfg.SpreadsheetID); err != nil {
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
	sheetIDs, err := sheets_util.LoadSheetIDMapping(svc, cfg.SpreadsheetID)
	if err != nil {
		return nil, err
	}

	var allSheets = []string{"flow_internal"}
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		allSheets = append(allSheets, res.Sheet)
	}

	var actions []*sheets.Request
	var description string

	for _, sheet := range allSheets {
		var _, exists = sheetIDs[sheet]

		if !exists && !isDelete {
			description += fmt.Sprintf("Created sheet %q.\n", sheet)

			actions = append(actions, &sheets.Request{
				AddSheet: &sheets.AddSheetRequest{
					Properties: &sheets.SheetProperties{
						Title: sheet,
					},
				},
			})
		} else if exists && isDelete {
			description += fmt.Sprintf("Deleted sheet %q.\n", sheet)

			actions = append(actions, &sheets.Request{
				DeleteSheet: &sheets.DeleteSheetRequest{
					SheetId: sheetIDs[sheet],
				},
			})
		}
	}

	if req.DryRun || len(actions) == 0 {
		// Nothing to do.
	} else {
		var batchUpdate = svc.Spreadsheets.BatchUpdate(cfg.SpreadsheetID,
			&sheets.BatchUpdateSpreadsheetRequest{
				Requests: actions,
			},
		)
		if _, err = batchUpdate.Do(); err != nil {
			return nil, fmt.Errorf("while updated sheets: %w", err)
		}
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
	sheetIDs, err := sheets_util.LoadSheetIDMapping(svc, cfg.SpreadsheetID)
	if err != nil {
		return err
	}
	bindings, err := loadBindingStates(
		svc,
		cfg.SpreadsheetID,
		sheetIDs,
		open.Open.Materialization.Bindings,
		checkpoint.Round,
	)
	if err != nil {
		return err
	}

	var transactor = &transactor{
		bindings:      bindings,
		client:        svc,
		round:         checkpoint.Round,
		spreadsheetId: cfg.SpreadsheetID,
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: nil},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return pm.RunTransactions(stream, transactor, logrus.NewEntry(logrus.StandardLogger()))
}

// loadBindingStates fetches documents of each binding for the committed `round`,
// and returns bindings ready for use within the transactor.
func loadBindingStates(
	client *sheets.Service,
	spreadsheetID string,
	sheetIDs map[string]int64,
	bindings []*pf.MaterializationSpec_Binding,
	loadRound int64,
) ([]transactorBinding, error) {
	var states, err = sheets_util.LoadInternalStates(client, spreadsheetID)
	if err != nil {
		return nil, err
	}

	var out []transactorBinding
	for _, binding := range bindings {
		var sheetName = binding.ResourcePath[0]
		var sheetID, ok = sheetIDs[sheetName]
		if !ok {
			return nil, fmt.Errorf("required sheet %q doesn't exist", sheetName)
		}

		out = append(out, transactorBinding{
			Docs:          []json.RawMessage(nil),
			Fields:        binding.FieldSelection,
			StateColumn:   [2]int64{-1, -1},
			StateSheetId:  states.SheetID,
			UserSheetId:   sheetID,
			UserSheetName: sheetName,
		})
	}

	for _, state := range states.Columns {
		for bindInd := range out {
			if state.UserSheetName != out[bindInd].UserSheetName {
				continue
			}

			// Track the sheet column in which even/odd state lives for each binding.
			out[bindInd].StateColumn[state.Round%2] = state.ColInd

			switch {
			case state.Round+1 == loadRound:
				continue // This is a prior transaction to the one being loaded.
			case state.Round == loadRound+1:
				continue // This transaction was attempted but was rolled back prior to committing.
			case state.Round == loadRound:
				// Fall through.
				out[bindInd].Docs = state.Docs
			default:
				return nil, fmt.Errorf("column %d with sheet %q has an unexpected round %d (loading %d)",
					state.ColInd, state.UserSheetName, state.Round, loadRound)
			}
		}
	}

	// Assign sheet columns for even/odd states which are not yet in the `flow_internal` sheet.
	var nextCol = int64(len(states.Columns))

	for ind := range out {
		if out[ind].StateColumn[0] == -1 {
			out[ind].StateColumn[0] = nextCol
			nextCol++
		}
		if out[ind].StateColumn[1] == -1 {
			out[ind].StateColumn[1] = nextCol
			nextCol++
		}
	}

	return out, nil
}

func main() { boilerplate.RunMain(new(driver)) }
