// Package mongodb implements the MongoDB storage adapter, BSON query compiler,
// aggregation pipeline generator, and Dataset Studio compiler.
//
// File: types.go
// Usage:
//   This file defines the type conversion mappings between core engine generic DataType
//   enums (model.TypeString, model.TypeInt, model.TypeDecimal, model.TypeJSON, etc.) and
//   MongoDB BSON types (string, int, long, double, bool, date, object, array, binData).
//   It also builds MongoDB collection $jsonSchema validator documents.
package mongodb

import (
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/schema"
)

// ToBSONType maps core DataType to MongoDB $jsonSchema bsonType.
//
// Purpose:
//   Translates a generic engine data type enum into the corresponding BSON type name string.
//
// Where it is used:
//   - Used by BuildJSONSchema and aggregation projection logic.
//
// When can it be used:
//   - When declaring BSON schema validation rules or type checks in MongoDB.
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
//
// Purpose:
//   Generates a native MongoDB $jsonSchema document enforcing property types and required fields on collections.
//
// Where it is used:
//   - Used during collection creation and schema validation rule updates on MongoAdapter.
//
// When can it be used:
//   - When establishing schema validation on a MongoDB collection.
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
