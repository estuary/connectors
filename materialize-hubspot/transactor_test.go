package hubspot

import (
	"encoding/json"
	"testing"

	"github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

type FakeStream struct {
	index int
	items []StoreIteratorItem
}

func (s *FakeStream) Send(*pm.Response) error {
	return nil
}

func (s *FakeStream) RecvMsg(request *pm.Request) error {
	request.Reset()
	if s.index < len(s.items) {
		item := s.items[s.index]
		_ = item

		request.Store = &pm.Request_Store{
			Binding:      uint32(item.Binding),
			KeyPacked:    item.Key.Pack(),
			ValuesPacked: item.Values.Pack(),
			DocJson:      item.RawJSON,
			Exists:       item.Exists,
		}

		s.index++
		return nil
	}

	return nil
}

type StoreIteratorItem struct {
	Binding int
	Key     tuple.Tuple
	Values  tuple.Tuple
	RawJSON json.RawMessage
	Exists  bool
}

func NewStoreIterator(t *testing.T, data []StoreIteratorItem) *materialize.StoreIterator {
	t.Helper()
	stream := &FakeStream{
		index: 0,
		items: data,
	}

	request := pm.Request{
		Store: &pm.Request_Store{},
	}

	it := materialize.NewStoreIterator(t.Context(), stream, &request)
	return it
}

func TestStoreBatches(t *testing.T) {
	tests := []struct {
		name     string
		bindings []*binding
		items    []StoreIteratorItem
		size     int
		expected []*Batch
	}{
		{
			name: "simple",
			bindings: []*binding{
				{
					object:     CompaniesObject,
					properties: companiesProperties,
					idProperty: companiesProperties["domain"],
					fields: []*MappedField{
						{
							Property: companiesProperties["domain"],
						},
						{
							Property: companiesProperties["name"],
						},
					},
					docField: nil,
				},
			},
			items: []StoreIteratorItem{
				{
					Binding: 0,
					Key: tuple.Tuple{
						"initech.net",
					},
					Values: tuple.Tuple{
						"Initech",
					},
					Exists: false,
				},
				{
					Binding: 0,
					Key: tuple.Tuple{
						"vandelayindustries.com",
					},
					Values: tuple.Tuple{
						"Vandelay Industries",
					},
					Exists: false,
				},
			},
			size: 5,
			expected: []*Batch{
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "initech.net",
							Properties: map[string]any{
								"domain": "initech.net",
								"name":   "Initech",
							},
						},
						{
							ID: "vandelayindustries.com",
							Properties: map[string]any{
								"domain": "vandelayindustries.com",
								"name":   "Vandelay Industries",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple items",
			bindings: []*binding{
				{
					object:     CompaniesObject,
					properties: companiesProperties,
					idProperty: companiesProperties["domain"],
					fields: []*MappedField{
						{
							Property: companiesProperties["domain"],
						},
						{
							Property: companiesProperties["name"],
						},
						{
							Property: companiesProperties["city"],
						},
					},
					docField: nil,
				},
			},
			items: []StoreIteratorItem{
				{
					Binding: 0,
					Key: tuple.Tuple{
						"initech.net",
					},
					Values: tuple.Tuple{
						"Initech",
						nil,
					},
					Exists: false,
				},
				{
					Binding: 0,
					Key: tuple.Tuple{
						"initech.net",
					},
					Values: tuple.Tuple{
						nil,
						"Austin",
					},
					Exists: false,
				},
			},
			size: 5,
			expected: []*Batch{
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "initech.net",
							Properties: map[string]any{
								"domain": "initech.net",
								"name":   "Initech",
								"city":   "Austin",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple batches per binding",
			bindings: []*binding{
				{
					object:     CompaniesObject,
					properties: companiesProperties,
					idProperty: companiesProperties["domain"],
					fields: []*MappedField{
						{
							Property: companiesProperties["domain"],
						},
						{
							Property: companiesProperties["name"],
						},
					},
					docField: nil,
				},
			},
			items: []StoreIteratorItem{
				{
					Binding: 0,
					Key: tuple.Tuple{
						"initech.net",
					},
					Values: tuple.Tuple{
						"Initech",
					},
				},
				{
					Binding: 0,
					Key: tuple.Tuple{
						"cyberdyne.io",
					},
					Values: tuple.Tuple{
						"Cyberdyne Systems",
					},
				},
				{
					Binding: 0,
					Key: tuple.Tuple{
						"aperturescience.com",
					},
					Values: tuple.Tuple{
						"Aperture Science",
					},
				},
			},
			size: 2,
			expected: []*Batch{
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "initech.net",
							Properties: map[string]any{
								"domain": "initech.net",
								"name":   "Initech",
							},
						},
						{
							ID: "cyberdyne.io",
							Properties: map[string]any{
								"domain": "cyberdyne.io",
								"name":   "Cyberdyne Systems",
							},
						},
					},
				},
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "aperturescience.com",
							Properties: map[string]any{
								"domain": "aperturescience.com",
								"name":   "Aperture Science",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple bindings",
			bindings: []*binding{
				{
					object:     CompaniesObject,
					properties: companiesProperties,
					idProperty: companiesProperties["domain"],
					fields: []*MappedField{
						{
							Property: companiesProperties["domain"],
						},
						{
							Property: companiesProperties["name"],
						},
					},
					docField: nil,
				},
				{
					object:     ContactsObject,
					properties: contactsProperties,
					idProperty: contactsProperties["email"],
					fields: []*MappedField{
						{
							Property: contactsProperties["email"],
						},
						{
							Property: contactsProperties["firstname"],
						},
						{
							Property: contactsProperties["lastname"],
						},
					},
					docField: nil,
				},
			},
			items: []StoreIteratorItem{
				{
					Binding: 0,
					Key: tuple.Tuple{
						"initech.net",
					},
					Values: tuple.Tuple{
						"Initech",
					},
					Exists: false,
				},
				{
					Binding: 1,
					Key: tuple.Tuple{
						"pgibbons@initech.net",
					},
					Values: tuple.Tuple{
						"Peter",
						"Gibbons",
					},
					Exists: false,
				},
			},
			size: 5,
			expected: []*Batch{
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "initech.net",
							Properties: map[string]any{
								"domain": "initech.net",
								"name":   "Initech",
							},
						},
					},
				},
				{
					BindingIdx: 1,
					Items: []BatchItem{
						{
							ID: "pgibbons@initech.net",
							Properties: map[string]any{
								"email":     "pgibbons@initech.net",
								"firstname": "Peter",
								"lastname":  "Gibbons",
							},
						},
					},
				},
			},
		},
		{
			name: "empty string key",
			bindings: []*binding{
				{
					object:     CompaniesObject,
					properties: companiesProperties,
					idProperty: companiesProperties["domain"],
					fields: []*MappedField{
						{
							Property: companiesProperties["domain"],
						},
						{
							Property: companiesProperties["name"],
						},
					},
					docField: nil,
				},
			},
			items: []StoreIteratorItem{
				{
					Binding: 0,
					Key: tuple.Tuple{
						"",
					},
					Values: tuple.Tuple{
						"Unknown Company",
					},
					Exists: false,
				},
				{
					Binding: 0,
					Key: tuple.Tuple{
						"initech.net",
					},
					Values: tuple.Tuple{
						"Initech",
					},
					Exists: false,
				},
			},
			size: 5,
			expected: []*Batch{
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "",
							Properties: map[string]any{
								"domain": "",
								"name":   "Unknown Company",
							},
						},
						{
							ID: "initech.net",
							Properties: map[string]any{
								"domain": "initech.net",
								"name":   "Initech",
							},
						},
					},
				},
			},
		},
		{
			// Since this connector works only in delta-update mode, we don't
			// have the ability to do hard deletions or clear fields.
			name: "simple deletion",
			bindings: []*binding{
				{
					object:     ContactsObject,
					properties: contactsProperties,
					idProperty: contactsProperties["email"],
					fields: []*MappedField{
						{
							Name:     "email",
							Property: contactsProperties["email"],
						},
						{
							Name: "/_meta/op",
							Property: &Property{
								Name: "meta_op",
								Type: EnumPropertyType,
								Options: []PropertyOption{
									{
										Value: "c",
									},
									{
										Value: "d",
									},
									{
										Value: "u",
									},
								},
							},
						},
					},
					docField: nil,
				},
			},
			items: []StoreIteratorItem{
				{
					Binding: 0,
					Key: tuple.Tuple{
						"b.wayne@wayneent.com",
					},
					Values: tuple.Tuple{
						"d",
					},
					Exists: false,
				},
			},
			size: 5,
			expected: []*Batch{
				{
					BindingIdx: 0,
					Items: []BatchItem{
						{
							ID: "b.wayne@wayneent.com",
							Properties: map[string]any{
								"email":   "b.wayne@wayneent.com",
								"meta_op": "d",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewStoreIterator(t, tt.items)
			client, err := NewClientDefaultLimiter(Credentials{})
			require.NoError(t, err)

			transactor := &transactor{
				client:   client,
				bindings: tt.bindings,
			}

			i := 0
			for batch, err := range transactor.storeBatches(it, tt.size) {
				require.NoError(t, err)
				require.Equal(t, tt.expected[i], batch)
				i++
			}
		})
	}
}
