package hubspot

import (
	"context"
	"fmt"
)

type CRMObjectSchema struct {
	ObjectTypeID   ObjectTypeID
	PropertyGroups map[string]*PropertyGroup
	Properties     map[string]*Property
}

type CRMSchema struct {
	Objects map[ObjectName]*CRMObjectSchema
}

func NewCRMSchema() *CRMSchema {
	return &CRMSchema{Objects: make(map[ObjectName]*CRMObjectSchema)}
}

// Schema contains information about the current HubSpot configuration.
type Schema struct {
	CRM *CRMSchema
}

func LoadSchema(ctx context.Context, client *Client, resources []Resource) (*Schema, error) {
	var schema = &Schema{
		CRM: NewCRMSchema(),
	}

	var objectTypeIDs map[ObjectName]ObjectTypeID
	for _, resource := range resources {
		var object, err = resource.CRMObject()
		if err != nil {
			return nil, err
		}
		objectName, err := resource.ObjectName()
		if err != nil {
			return nil, err
		}

		var objectTypeID = ObjectTypeID(objectName)
		if object == CustomObjectObject {
			if objectTypeIDs == nil {
				objectTypeIDs, err = client.GetCustomObjectTypeIDs(ctx)
				if err != nil {
					return nil, err
				}
			}

			if id, ok := objectTypeIDs[objectName]; ok {
				objectTypeID = id
			} else {
				return nil, fmt.Errorf("definition not found for Custom Object: %q", objectName)
			}
		}

		groups, err := client.ListPropertyGroups(ctx, objectTypeID)
		if err != nil {
			return nil, err
		}
		var groupMap = make(map[string]*PropertyGroup, len(groups))
		for _, group := range groups {
			groupMap[group.Name] = group
		}

		properties, err := client.ListProperties(ctx, objectTypeID)
		if err != nil {
			return nil, err
		}
		var propMap = make(map[string]*Property, len(properties))
		for _, prop := range properties {
			propMap[prop.Name] = prop
		}

		schema.CRM.Objects[objectName] = &CRMObjectSchema{
			ObjectTypeID:   objectTypeID,
			PropertyGroups: groupMap,
			Properties:     propMap,
		}

	}

	return schema, nil
}

func (s *Schema) GetProperty(objectName ObjectName, propName string) (*Property, bool) {
	var objectSchema, ok = s.CRM.Objects[objectName]
	if !ok {
		return nil, false
	}

	property, ok := objectSchema.Properties[propName]
	return property, ok
}
