package mongodb

import (
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
)

// ToBSONType maps core DataType to MongoDB $jsonSchema bsonType.
func ToBSONType(t model.DataType) string {
	switch t {
	case model.TypeString, model.TypeText:
		return "string"
	case model.TypeInt:
		return "int"
	case model.TypeLong:
		return "long"
	case model.TypeFloat, model.TypeDecimal:
		return "double"
	case model.TypeBoolean:
		return "bool"
	case model.TypeDateTime, model.TypeDate:
		return "date"
	case model.TypeJSON:
		return "object"
	case model.TypeArray:
		return "array"
	case model.TypeBinary:
		return "binData"
	default:
		return "string"
	}
}

// BuildJSONSchema creates a MongoDB $jsonSchema validator document from a schema.
func BuildJSONSchema(s *schema.Schema) map[string]any {
	properties := make(map[string]any)
	var required []string

	for _, attr := range s.Attributes {
		prop := map[string]any{
			"bsonType": ToBSONType(attr.Type),
		}
		if attr.Comment != "" {
			prop["description"] = attr.Comment
		}
		properties[attr.Name] = prop

		if !attr.Nullable && !attr.AutoIncrement && !attr.PrimaryKey {
			required = append(required, attr.Name)
		}
	}

	validator := map[string]any{
		"$jsonSchema": map[string]any{
			"bsonType":   "object",
			"properties": properties,
		},
	}

	if len(required) > 0 {
		validator["$jsonSchema"].(map[string]any)["required"] = required
	}

	return validator
}
