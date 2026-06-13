package hubspot

import (
	"testing"

	"github.com/estuary/flow/go/protocols/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestValidateBinding(t *testing.T) {
	tests := []struct {
		name        string
		schema      *Schema
		resource    Resource
		binding     *pm.Request_Validate_Binding
		lastSpec    *pf.MaterializationSpec
		expectedErr string
		expected    map[string]*pm.Response_Validated_Constraint
	}{
		{
			name: "no bindings",
			schema: &Schema{
				CRM: NewCRMSchema(),
			},
			resource: Resource{
				Name:   "myschema_mycollection",
				Object: "Companies",
			},
			binding:  &pm.Request_Validate_Binding{},
			lastSpec: nil,
			expected: map[string]*pm.Response_Validated_Constraint{},
		},
		{
			name: "nullable key field no default",
			schema: &Schema{
				CRM: NewCRMSchema(),
			},
			resource: Resource{
				Name:   "myschema_mycollection",
				Object: "Contacts",
			},
			binding: &pm.Request_Validate_Binding{
				GroupBy: []string{"email"},
				Collection: flow.CollectionSpec{
					Name: "mycollection",
					Projections: []flow.Projection{
						{
							Ptr:          "/email",
							Field:        "email",
							IsPrimaryKey: true,
							Inference: flow.Inference{
								Exists: flow.Inference_MAY,
							},
						},
					},
				},
			},
			lastSpec:    nil,
			expectedErr: "cannot materialize collection 'mycollection' with nullable key field 'email' unless it has a default value annotation",
		},
		{
			name: "no keys selected",
			schema: &Schema{
				CRM: &CRMSchema{Objects: map[CRMObject]*CRMObjectSchema{
					CompaniesObject: &CRMObjectSchema{
						Properties: companiesProperties,
					},
				}},
			},
			resource: Resource{
				Name:   "myschema_mycollection",
				Object: "Companies",
			},
			binding: &pm.Request_Validate_Binding{
				GroupBy: []string{"howdy"},
				Collection: flow.CollectionSpec{
					Name: "mycollection",
					Projections: []flow.Projection{
						{
							Ptr:          "/howdy",
							Field:        "howdy",
							IsPrimaryKey: true,
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
						{
							Ptr:          "/name",
							Field:        "name",
							IsPrimaryKey: false,
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
					},
				},
			},
			lastSpec: nil,
			expected: map[string]*pm.Response_Validated_Constraint{
				"howdy": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason:      `A property for this field does not exist: "howdy"`,
					FoldedField: "howdy",
				},
				"name": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_OPTIONAL,
					Reason:      "This field is able to be materialized",
					FoldedField: "name",
				},
			},
		},
		{
			name: "one primary key",
			schema: &Schema{
				CRM: &CRMSchema{
					Objects: map[CRMObject]*CRMObjectSchema{
						CompaniesObject: &CRMObjectSchema{
							Properties: companiesProperties,
						},
					},
				},
			},
			resource: Resource{
				Name:   "myschema_mycollection",
				Object: "Companies",
			},
			binding: &pm.Request_Validate_Binding{
				GroupBy: []string{"domain"},
				Collection: flow.CollectionSpec{
					Name: "mycollection",
					Projections: []flow.Projection{
						{
							Ptr:          "/domain",
							Field:        "domain",
							IsPrimaryKey: true,
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
					},
				},
			},
			lastSpec: nil,
			expected: map[string]*pm.Response_Validated_Constraint{
				"domain": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_REQUIRED,
					Reason:      "One collections key field is required",
					FoldedField: "domain",
				},
			},
		},
		{
			name: "two keys not allowed",
			schema: &Schema{
				CRM: &CRMSchema{
					Objects: map[CRMObject]*CRMObjectSchema{
						ContactsObject: &CRMObjectSchema{
							Properties: contactsProperties,
						},
					},
				},
			},
			resource: Resource{
				Name:   "myschema_mycollection",
				Object: "Contacts",
			},
			binding: &pm.Request_Validate_Binding{
				GroupBy: []string{"email", "firstname"},
				Collection: flow.CollectionSpec{
					Name: "mycollection",
					Projections: []flow.Projection{
						{
							Ptr:          "/email",
							Field:        "email",
							IsPrimaryKey: true,
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
						{
							Ptr:          "/firstname",
							Field:        "firstname",
							IsPrimaryKey: true,
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
						{
							Ptr:   "/lastname",
							Field: "lastname",
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
					},
				},
			},
			lastSpec: nil,
			expected: map[string]*pm.Response_Validated_Constraint{
				"email": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_REQUIRED,
					Reason:      "One collections key field is required",
					FoldedField: "email",
				},
				"firstname": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason:      "Only one key field may be selected for record matching",
					FoldedField: "firstname",
				},
				"lastname": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_OPTIONAL,
					Reason:      "This field is able to be materialized",
					FoldedField: "lastname",
				},
			},
		},
		{
			name: "incompatible property type",
			schema: &Schema{
				CRM: &CRMSchema{
					Objects: map[CRMObject]*CRMObjectSchema{
						ContactsObject: &CRMObjectSchema{
							Properties: contactsProperties,
						},
					},
				},
			},
			resource: Resource{
				Name:   "myschema_mycollection",
				Object: "Contacts",
			},
			binding: &pm.Request_Validate_Binding{
				GroupBy: []string{"email"},
				Collection: flow.CollectionSpec{
					Name: "mycollection",
					Projections: []flow.Projection{
						{
							Ptr:          "/email",
							Field:        "email",
							IsPrimaryKey: true,
							Inference: flow.Inference{
								Exists: flow.Inference_MUST,
							},
						},
						{
							Ptr:   "/followercount",
							Field: "followercount",
							Inference: flow.Inference{
								Types:  []string{"string"},
								Exists: flow.Inference_MUST,
								String_: &flow.Inference_String{
									Format: "",
								},
							},
						},
					},
				},
			},
			lastSpec: nil,
			expected: map[string]*pm.Response_Validated_Constraint{
				"email": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_REQUIRED,
					Reason:      "One collections key field is required",
					FoldedField: "email",
				},
				"followercount": &pm.Response_Validated_Constraint{
					Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
					Reason:      `The existing property for this field has an incompatible type: "number"`,
					FoldedField: "followercount",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewValidator(tt.schema)
			constraints, err := validator.ValidateBinding(tt.resource, tt.binding, tt.lastSpec)
			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Equal(t, tt.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected, constraints)
		})
	}
}
