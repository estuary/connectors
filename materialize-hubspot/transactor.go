package main

import (
	"context"
	"encoding/json"

	"github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/pingcap/log"
)

type transactor struct {
	config *Config
}

var _ boilerplate.MaterializerTransactor = (*transactor)(nil)

func (t *transactor) RecoverCheckpoint(
	ctx context.Context,
	spec pf.MaterializationSpec,
	rangeSpec pf.RangeSpec,
) (boilerplate.RuntimeCheckpoint, error) {
	log.Info("recover checkpoint")
	return nil, nil
}

func (t *transactor) UnmarshalState(_ json.RawMessage) error {
	log.Info("unmarshal state")
	return nil
}

func (t *transactor) Load(it *materialize.LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	log.Info("load")
	for it.Next() {
		log.Info("load: item")
	}
	return nil
}

func (t *transactor) Store(it *materialize.StoreIterator) (materialize.StartCommitFunc, error) {
	log.Info("store")
	for it.Next() {
		log.Info("store: item")
	}
	return nil, nil
}

func (t *transactor) Acknowledge(_ context.Context) (*pf.ConnectorState, error) {
	log.Info("acknowledge")
	return nil, nil
}

func (t *transactor) Destroy() {
}
