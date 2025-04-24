package schemagen

import "github.com/invopop/jsonschema"

// OneOfSchema builds a JSON schema for a set of subschemas that should be part
// of a "oneOf" combination. This can be used to represent multiple choices for
// something like authentication options, where one and only one option should
// be configured. `inputs` is the list of different options that should be
// represented; use the OneofSubSchema helper function to create these.
//
// See one_of_test.go for an example of using this.
func OneOfSchema(title, description, discriminator, default_ string, inputs ...oneOfSubSchema) *jsonschema.Schema {
	var oneOfs []*jsonschema.Schema

	for _, input := range inputs {
		config := GenerateSchema(input.title, input.configObj)
		config.Properties.Set(discriminator, &jsonschema.Schema{
			Type:    "string",
			Default: input.default_,
			Const:   input.default_,
			Extras:  map[string]any{"order": 0},
		})
		config.Properties.MoveToFront(discriminator)
		oneOfs = append(oneOfs, config)
	}

	return &jsonschema.Schema{
		Title:       title,
		Description: description,
		Default:     map[string]string{discriminator: default_},
		OneOf:       oneOfs,
		Extras: map[string]any{
			"discriminator": map[string]string{"propertyName": discriminator},
		},
		Type: "object",
	}
}

// OneOfSubSchema builds a subschema to be included in a "OneOf" combination.
func OneOfSubSchema(title string, configObj any, default_ string) oneOfSubSchema {
	return oneOfSubSchema{title: title, configObj: configObj, default_: default_}
}

type oneOfSubSchema struct {
	title     string
	configObj any
	default_  string
}
