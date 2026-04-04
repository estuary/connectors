package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/connectors/go/materialize"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/consumer/protocol"

	log "github.com/sirupsen/logrus"
)

type TokenUpdate struct {
	RefreshToken         string    `json:"refresh_token"`
	AccessToken          string    `json:"access_token"`
	AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
}

type Checkpoint struct {
	TokenUpdate *TokenUpdate `json:"token_update"`
}

type transactor struct {
	client     *Client
	config     *Config
	checkpoint *Checkpoint
}

var _ boilerplate.MaterializerTransactor = (*transactor)(nil)

func (t *transactor) RecoverCheckpoint(
	ctx context.Context,
	spec pf.MaterializationSpec,
	rangeSpec pf.RangeSpec,
) (boilerplate.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &t.checkpoint); err != nil {
		return fmt.Errorf("unmarshaling checkpoint state: %w", err)
	}

	if t.checkpoint.TokenUpdate != nil {
		log.WithFields(log.Fields{
			"refresh_token":           t.checkpoint.TokenUpdate.RefreshToken,
			"access_token":            t.checkpoint.TokenUpdate.AccessToken,
			"access_token_expires_at": t.checkpoint.TokenUpdate.AccessTokenExpiresAt,
		}).Info("loaded checkpoint")
		t.client.UpdateTokens(t.checkpoint.TokenUpdate)
	}
	return nil
}

func (t *transactor) Load(it *materialize.LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	log.Info("load")
	ctx := it.Context()

	values, err := t.client.ListObjects(ctx, ContactsObject)
	if err != nil {
		return err
	}

	log.Infof("%v", values)

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

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		t.checkpoint.TokenUpdate = t.client.TokenUpdate()

		var checkpointJSON, err = json.Marshal(t.checkpoint)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("updating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	err := t.client.RefreshToken(ctx, time.Now())
	if err != nil {
		return nil, err
	}
	t.checkpoint.TokenUpdate = t.client.TokenUpdate()

	checkpointJSON, err := json.Marshal(t.checkpoint)
	if err != nil {
		return nil, fmt.Errorf("updating checkpoint json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
}

func (t *transactor) Destroy() {
}
