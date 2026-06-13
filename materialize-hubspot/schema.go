package hubspot

import "context"

type CRMObjectSchema struct {
	PropertyGroups map[string]*PropertyGroup
	Properties     map[string]*Property
}

func NewCRMObjectSchema() *CRMObjectSchema {
	return &CRMObjectSchema{
		PropertyGroups: make(map[string]*PropertyGroup),
		Properties:     make(map[string]*Property),
	}
}

type CRMSchema struct {
	Objects map[CRMObject]*CRMObjectSchema
}

func NewCRMSchema() *CRMSchema {
	return &CRMSchema{Objects: make(map[CRMObject]*CRMObjectSchema)}
}

// Schema contains information about the current HubSpot configuration.
type Schema struct {
	CRM *CRMSchema
}

func LoadSchema(ctx context.Context, client *Client, resources []Resource) (*Schema, error) {
	schema := &Schema{
		CRM: NewCRMSchema(),
	}
	for _, resource := range resources {
		object, err := resource.CRMObject()
		if err != nil {
			return nil, err
		}

		groups, err := client.ListPropertyGroups(ctx, object)
		if err != nil {
			return nil, err
		}
		groupMap := make(map[string]*PropertyGroup, len(groups))
		for _, group := range groups {
			groupMap[group.Name] = group
		}

		properties, err := client.ListProperties(ctx, object)
		if err != nil {
			return nil, err
		}
		propMap := make(map[string]*Property, len(properties))
		for _, prop := range properties {
			propMap[prop.Name] = prop
		}

		schema.CRM.Objects[object] = &CRMObjectSchema{
			PropertyGroups: groupMap,
			Properties:     propMap,
		}

	}

	return schema, nil
}

func (s *Schema) AddPropertyGroup(object CRMObject, group *PropertyGroup) {
	objectSchema, ok := s.CRM.Objects[object]
	if !ok {
		objectSchema = NewCRMObjectSchema()
		s.CRM.Objects[object] = objectSchema
	}
	objectSchema.PropertyGroups[group.Name] = group
}

func (s *Schema) HasPropertyGroup(object CRMObject, groupName string) bool {
	objectSchema, ok := s.CRM.Objects[object]
	if !ok {
		return false
	}

	_, ok = objectSchema.PropertyGroups[groupName]
	return ok
}

func (s *Schema) GetProperty(object CRMObject, propName string) (*Property, bool) {
	objectSchema, ok := s.CRM.Objects[object]
	if !ok {
		return nil, false
	}

	property, ok := objectSchema.Properties[propName]
	return property, ok
}
