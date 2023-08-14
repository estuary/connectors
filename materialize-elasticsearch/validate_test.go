package main

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var (
	testProjectionRootDoc = pf.Projection{
		Ptr:   "",
		Field: "flow_document",
		Inference: pf.Inference{
			Types:       []string{"object"},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MUST,
		},
	}

	testProjectionStringKey = pf.Projection{
		Ptr:          "/stringKey",
		Field:        "stringKey",
		IsPrimaryKey: true,
		Inference: pf.Inference{
			Types:       []string{"string"},
			String_:     &pf.Inference_String{},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MUST,
		},
	}

	testProjectionIntKey = pf.Projection{
		Ptr:          "/intKey",
		Field:        "intKey",
		IsPrimaryKey: true,
		Inference: pf.Inference{
			Types:       []string{"integer"},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MUST,
		},
	}

	testProjectionStringFormatIntVal = pf.Projection{
		Ptr:   "/strFormatInt",
		Field: "strFormatInt",
		Inference: pf.Inference{
			Types: []string{"string"},
			String_: &pf.Inference_String{
				Format:          "integer",
				ContentEncoding: "",
			},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MAY,
		},
	}

	testProjectionBoolVal = pf.Projection{
		Ptr:   "/bool",
		Field: "bool",
		Inference: pf.Inference{
			Types:       []string{"boolean"},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MAY,
		},
	}

	testProjectionObjVal = pf.Projection{
		Ptr:   "/object",
		Field: "object",
		Inference: pf.Inference{
			Types:       []string{"object"},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MAY,
		},
	}

	testProjectionNullVal = pf.Projection{
		Ptr:   "/null",
		Field: "null",
		Inference: pf.Inference{
			Types:       []string{"null"},
			DefaultJson: []byte{},
			Exists:      pf.Inference_MAY,
		},
	}
)

func TestValdiateSelectedFields(t *testing.T) {
	collection := testCollection("some/collection",
		testProjectionStringKey,
		testProjectionIntKey,
		testProjectionStringFormatIntVal,
		testProjectionBoolVal,
		testProjectionRootDoc,
		testProjectionNullVal,
	)

	for _, tt := range []struct {
		name            string
		deltaUpdates    bool
		boundCollection pf.CollectionSpec
		fieldSelection  pf.FieldSelection
		wantErr         string
	}{
		{
			name:            "happy path",
			boundCollection: collection,
			deltaUpdates:    false,
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"strFormatInt", "bool"},
				Document: "flow_document",
			},
			wantErr: "",
		},
		{
			name:            "recommend fields not included",
			boundCollection: collection,
			deltaUpdates:    false,
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{},
				Document: "flow_document",
			},
			wantErr: "",
		},
		{
			name:            "missing key",
			boundCollection: collection,
			deltaUpdates:    false,
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey"},
				Values:   []string{"strFormatInt", "bool"},
				Document: "flow_document",
			},
			wantErr: "This field is a key in the current materialization",
		},
		{
			name: "root document projection change is not allowed for standard updates",
			boundCollection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionStringFormatIntVal,
				testProjectionBoolVal,
				pf.Projection{
					Ptr:   "",
					Field: "changed_flow_document",
					Inference: pf.Inference{
						Types:       []string{"object"},
						DefaultJson: []byte{},
						Exists:      pf.Inference_MUST,
					},
				},
				testProjectionNullVal,
			),
			deltaUpdates: false,
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"strFormatInt", "bool"},
				Document: "changed_flow_document",
			},
			wantErr: "The root document must continue to be materialized as the same projected field",
		},
		{
			name:            "no root document selected for delta updates",
			boundCollection: collection,
			deltaUpdates:    true,
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"strFormatInt", "bool"},
				Document: "",
			},
			wantErr: "",
		},
		{
			name:            "forbidden field selected",
			boundCollection: collection,
			deltaUpdates:    false,
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"strFormatInt", "bool", "null"},
				Document: "flow_document",
			},
			wantErr: "Cannot materialize this field",
		},
		{
			name:            "non-existent field selected",
			boundCollection: collection,
			deltaUpdates:    false,
			fieldSelection: pf.FieldSelection{
				Keys:   []string{"stringKey", "intKey"},
				Values: []string{"bogus", "bool"},
			},
			wantErr: "no such projection for field 'bogus'",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			binding := &pf.MaterializationSpec_Binding{
				ResourcePath:   []string{"some-index"},
				Collection:     tt.boundCollection,
				FieldSelection: tt.fieldSelection,
			}

			res := resource{
				Index:        "some-index",
				DeltaUpdates: tt.deltaUpdates,
			}

			storedSpec := &pf.MaterializationSpec{
				Name: "test_materialization",
				Bindings: []*pf.MaterializationSpec_Binding{
					{
						ResourcePath: []string{"some-index"},
						Collection:   collection,
						FieldSelection: pf.FieldSelection{
							Keys:     []string{"stringKey", "intKey"},
							Values:   []string{"strFormatInt", "bool"},
							Document: "flow_document",
						},
						DeltaUpdates: false,
					},
				},
			}

			err := validateSelectedFields(res, binding, storedSpec)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}

	t.Run("standard updates with no root document", func(t *testing.T) {
		binding := &pf.MaterializationSpec_Binding{
			ResourcePath: []string{"some-index"},
			Collection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionStringFormatIntVal,
				testProjectionBoolVal,
				testProjectionRootDoc,
				testProjectionNullVal,
			),
			FieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"strFormatInt", "bool"},
				Document: "",
			},
		}

		res := resource{
			Index:        "some-index",
			DeltaUpdates: false,
		}

		err := validateSelectedFields(res, binding, nil)
		require.ErrorContains(t, err, "the materialization must include a projection of the root document, but no such projection is included. It is required because: The root document is required for a standard updates materialization")
	})

	t.Run("delta updates to standard updates is not allowed", func(t *testing.T) {
		binding := &pf.MaterializationSpec_Binding{
			ResourcePath: []string{"some-index"},
			Collection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionStringFormatIntVal,
				testProjectionBoolVal,
				testProjectionRootDoc,
				testProjectionNullVal,
			),
			FieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"strFormatInt", "bool"},
				Document: "flow_document",
			},
		}

		res := resource{
			Index:        "some-index",
			DeltaUpdates: false,
		}

		storedSpec := &pf.MaterializationSpec{
			Name: "test_materialization",
			Bindings: []*pf.MaterializationSpec_Binding{
				{
					ResourcePath: []string{"some-index"},
					Collection:   collection,
					FieldSelection: pf.FieldSelection{
						Keys:     []string{"stringKey", "intKey"},
						Values:   []string{"strFormatInt", "bool"},
						Document: "flow_document",
					},
					DeltaUpdates: true,
				},
			},
		}

		err := validateSelectedFields(res, binding, storedSpec)
		require.ErrorContains(t, err, "changing binding of collection some/collection from delta updates to standard updates is not allowed")
	})
}

func TestValidateBinding(t *testing.T) {
	collection := testCollection("some/collection",
		testProjectionStringKey,
		testProjectionIntKey,
		testProjectionStringFormatIntVal,
		testProjectionBoolVal,
		testProjectionRootDoc,
		testProjectionObjVal,
		testProjectionNullVal,
	)

	storedSpec := func() *pf.MaterializationSpec {
		return &pf.MaterializationSpec{
			Name: "test_materialization",
			Bindings: []*pf.MaterializationSpec_Binding{
				{
					ResourcePath: []string{"some-index"},
					Collection:   collection,
					FieldSelection: pf.FieldSelection{
						Keys:     []string{"stringKey", "intKey"},
						Values:   []string{"strFormatInt", "bool"},
						Document: "flow_document",
					},
					DeltaUpdates: false,
				},
			},
		}
	}

	tests := []struct {
		name            string
		deltaUpdates    bool
		storedSpec      func(*pf.MaterializationSpec) *pf.MaterializationSpec
		boundCollection pf.CollectionSpec
		wantErr         string
	}{
		{
			name:            "new matches existing",
			deltaUpdates:    false,
			storedSpec:      func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: collection,
			wantErr:         "",
		},
		{
			name:         "new with no existing",
			deltaUpdates: false,
			storedSpec: func(s *pf.MaterializationSpec) *pf.MaterializationSpec {
				s.Bindings[0].Collection = testCollection("different/collection",
					testProjectionStringKey,
					testProjectionIntKey,
					testProjectionRootDoc,
				)
				s.Bindings[0].ResourcePath = []string{"different-target"}
				return s
			},
			boundCollection: collection,
			wantErr:         "",
		},
		{
			name:            "standard to delta",
			deltaUpdates:    true,
			storedSpec:      func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: collection,
			wantErr:         "",
		},
		{
			name:         "delta to standard",
			deltaUpdates: false,
			storedSpec: func(s *pf.MaterializationSpec) *pf.MaterializationSpec {
				s.Bindings[0].DeltaUpdates = true
				return s
			},
			boundCollection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionStringFormatIntVal,
				pf.Projection{
					Ptr:   "/bool",
					Field: "bool",
					Inference: pf.Inference{
						Types:       []string{"string"},
						String_:     &pf.Inference_String{},
						DefaultJson: []byte{},
						Exists:      pf.Inference_MAY,
					},
				},
				testProjectionObjVal,
				testProjectionRootDoc,
				testProjectionNullVal,
			),
			wantErr: "changing binding of collection some/collection from delta updates to standard updates is not allowed",
		},
		{
			name:         "target conflict",
			deltaUpdates: false,
			storedSpec:   func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: testCollection("other/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionRootDoc,
			),
			wantErr: "is already materializing to index",
		},
		{
			name:         "unsatisfiable type change",
			deltaUpdates: false,
			storedSpec:   func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionStringFormatIntVal,
				pf.Projection{
					Ptr:   "/bool",
					Field: "bool",
					Inference: pf.Inference{
						Types:       []string{"string"},
						String_:     &pf.Inference_String{},
						DefaultJson: []byte{},
						Exists:      pf.Inference_MAY,
					},
				},
				testProjectionObjVal,
				testProjectionRootDoc,
				testProjectionNullVal,
			),
			wantErr: "",
		},
		{
			name:         "forbidden type change",
			deltaUpdates: false,
			storedSpec:   func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionStringFormatIntVal,
				pf.Projection{
					Ptr:   "/bool",
					Field: "bool",
					Inference: pf.Inference{
						Types:       []string{"null"},
						DefaultJson: []byte{},
						Exists:      pf.Inference_MAY,
					},
				},
				testProjectionObjVal,
				testProjectionRootDoc,
				testProjectionNullVal,
			),
			wantErr: "",
		},
		{
			name:         "unsatisfiable format change",
			deltaUpdates: false,
			storedSpec:   func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				pf.Projection{
					Ptr:   "/strFormatInt",
					Field: "strFormatInt",
					Inference: pf.Inference{
						Types: []string{"string"},
						String_: &pf.Inference_String{
							Format:          "",
							ContentEncoding: "",
						},
						DefaultJson: []byte{},
						Exists:      pf.Inference_MAY,
					},
				},
				testProjectionBoolVal,
				testProjectionObjVal,
				testProjectionRootDoc,
				testProjectionNullVal,
			),
			wantErr: "",
		},
		{
			name:         "removed fields",
			deltaUpdates: false,
			storedSpec:   func(s *pf.MaterializationSpec) *pf.MaterializationSpec { return s },
			boundCollection: testCollection("some/collection",
				testProjectionStringKey,
				testProjectionIntKey,
				testProjectionRootDoc,
			),
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := resource{
				Index:        "some-index",
				DeltaUpdates: tt.deltaUpdates,
			}

			c, err := validateBinding(res, tt.boundCollection, tt.storedSpec(storedSpec()))
			if tt.wantErr == "" {
				require.NoError(t, err)
				formatted, err := json.MarshalIndent(c, "", "\t")
				require.NoError(t, err)
				cupaloy.SnapshotT(t, string(formatted))
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}

		})
	}
}

func testCollection(name string, projections ...pf.Projection) pf.CollectionSpec {
	sort.Slice(projections, func(i, j int) bool {
		return projections[i].Field < projections[j].Field
	})

	return pf.CollectionSpec{
		Name:        pf.Collection(name),
		Projections: projections,
	}
}
