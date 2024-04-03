package connector

import (
	"fmt"

	sql "github.com/estuary/connectors/materialize-sql"
	"google.golang.org/protobuf/types/descriptorpb"
)

func getTableDescriptorProto(table sql.Table) descriptorpb.DescriptorProto {
	var cols = table.Columns()

	var dp = descriptorpb.DescriptorProto{
		Name:  &table.Identifier,
		Field: make([]*descriptorpb.FieldDescriptorProto, len(cols)),
	}

	for i, col := range cols {
		var t descriptorpb.FieldDescriptorProto_Type
		var flatType, _ = col.AsFlatType()
		switch flatType {
		case sql.ARRAY:
			t = descriptorpb.FieldDescriptorProto_TYPE_STRING
		case sql.BINARY:
			t = descriptorpb.FieldDescriptorProto_TYPE_BYTES
		case sql.BOOLEAN:
			t = descriptorpb.FieldDescriptorProto_TYPE_BOOL
		case sql.INTEGER:
			t = descriptorpb.FieldDescriptorProto_TYPE_INT64
		case sql.MULTIPLE:
			t = descriptorpb.FieldDescriptorProto_TYPE_STRING
		case sql.OBJECT:
			t = descriptorpb.FieldDescriptorProto_TYPE_STRING
		case sql.STRING:
			t = descriptorpb.FieldDescriptorProto_TYPE_STRING
		case sql.NUMBER:
			t = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
		default:
			panic(fmt.Sprintf("unknown type in getTableDescriptorProto: %v", col))
		}

		dp.Field[i] = &descriptorpb.FieldDescriptorProto{
			Name: &col.Field,
			Type: &t,
		}
	}

	return dp
}
