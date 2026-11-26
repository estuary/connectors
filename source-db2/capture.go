package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/estuary/connectors/go/common"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// sqlcapture.Capture.activatePendingStreams selects a backfill mode before
// ActivateTable, so discovery and validation need the binding's effective key.
type db2CaptureDriver struct{ sqlcapture.Driver }

func (d *db2CaptureDriver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var driver = d.forCapture(open.Capture)
	return driver.Pull(open, stream)
}

func (d *db2CaptureDriver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var capture pf.CaptureSpec
	for _, binding := range req.Bindings {
		capture.Bindings = append(capture.Bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: binding.ResourceConfigJson, Collection: *req.BindingCollection(binding),
		})
	}
	var driver = d.forCapture(&capture)
	return driver.Validate(ctx, req)
}

func (d *db2CaptureDriver) forCapture(capture *pf.CaptureSpec) sqlcapture.Driver {
	var driver = d.Driver
	driver.Connect = func(ctx context.Context, name string, cfg json.RawMessage, createdAt common.CreatedAt) (sqlcapture.Database, error) {
		var db, err = d.Connect(ctx, name, cfg, createdAt)
		if err == nil {
			err = db.(interface{ setCapture(*pf.CaptureSpec) error }).setCapture(capture)
			if err != nil {
				db.Close(ctx)
			}
		}
		return db, err
	}
	return driver
}

func (db *db2Database) setCapture(capture *pf.CaptureSpec) error {
	db.bindingKeys = make(map[sqlcapture.StreamID][]string)
	for _, binding := range capture.Bindings {
		var resource sqlcapture.Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &resource); err != nil {
			return err
		}
		// Mirror key precedence in sqlcapture.Capture.activatePendingStreams.
		var key []string
		if collection := capture.BindingCollection(binding); !slices.Equal(collection.Key, db.FallbackCollectionKey()) {
			for _, ptr := range collection.Key {
				key = append(key, strings.ReplaceAll(strings.ReplaceAll(strings.TrimPrefix(ptr, "/"), "~1", "/"), "~0", "~"))
			}
		}
		if len(resource.PrimaryKey) > 0 {
			key = resource.PrimaryKey
		}
		if resource.Mode == sqlcapture.BackfillModeWithoutKey {
			key = nil
		}
		db.bindingKeys[sqlcapture.JoinStreamID(resource.Namespace, resource.Stream)] = key
	}
	return nil
}

func (db *db2Database) tableKey(info *sqlcapture.DiscoveryInfo) []string {
	if key, ok := db.bindingKeys[sqlcapture.JoinStreamID(info.Schema, info.Name)]; ok {
		return key
	}
	return info.PrimaryKey
}
