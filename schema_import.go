package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SanjayDrop5528/models-go-engine/model"
)

// SchemaImportResult contains all models and fields generated from the MongoDB JSON Schema,
// separating the root collection model from reusable embedded submodels.
type SchemaImportResult struct {
	Model     *model.ModelConfig   `json:"model"`
	Fields    []*model.DataModel   `json:"fields"`
	SubModels []*model.ModelConfig `json:"sub_models,omitempty"`
	SubFields []*model.DataModel   `json:"sub_fields,omitempty"`
	AllModels []*model.ModelConfig `json:"all_models"`
	AllFields []*model.DataModel   `json:"all_fields"`
}

// ImportJSONSchema parses a MongoDB JSON Schema (e.g. from MongoDB Compass or Atlas schema exports)
// and converts it into ModelConfigs and DataModel fields directly inside the adapter,
// creating reusable submodels for nested objects and array item objects based on sorted joined field names.
func (a *MongoAdapter) ImportJSONSchema(ctx context.Context, collectionName string, rawSchema any) (*SchemaImportResult, error) {
	return ParseMongoJSONSchema(collectionName, rawSchema)
}

type schemaParserContext struct {
	createdModels map[string]*model.ModelConfig
	modelFields   map[string][]*model.DataModel
	orderedModels []*model.ModelConfig
	defs          map[string]any
	now           time.Time
}

// ParseMongoJSONSchema converts a raw JSON Schema (string, []byte, or map[string]any) into
// engine ModelConfig and DataModel field catalogs.
//
// Rules:
// 1. Inputs: required: []string and properties / property: map[string]any.
// 2. Map keys are column names, property.x-bsonType is the data type.
// 3. For arrays with items of x-bsonType object, items are treated as separate models and parsed recursively.
// 4. For subobjects and nested objects, separate models are created.
// 5. Model names for subobjects/array items are formed by sorting field names ascending and joining them with "_",
//    allowing identical structures (e.g. shop_image and street_view_image) to be reused rather than duplicated.
// 6. Default values are NOT populated from samples or schema hints.
func ParseMongoJSONSchema(collectionName string, rawSchema any) (*SchemaImportResult, error) {
	if rawSchema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	var rootMap map[string]any

	switch v := rawSchema.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("empty schema string")
		}
		if err := json.Unmarshal([]byte(v), &rootMap); err != nil {
			return nil, fmt.Errorf("invalid json schema: %w", err)
		}
	case []byte:
		if len(v) == 0 {
			return nil, fmt.Errorf("empty schema bytes")
		}
		if err := json.Unmarshal(v, &rootMap); err != nil {
			return nil, fmt.Errorf("invalid json schema: %w", err)
		}
	case map[string]any:
		rootMap = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("unsupported schema data type: %T", v)
		}
		if err := json.Unmarshal(b, &rootMap); err != nil {
			return nil, fmt.Errorf("failed converting schema map: %w", err)
		}
	}

	// Support wrapper object format: { "collection": "claims", "schema": { "type": "object", ... } }
	schemaObj := rootMap
	if innerSchema, ok := rootMap["schema"].(map[string]any); ok && innerSchema != nil {
		schemaObj = innerSchema
		if collectionName == "" {
			for _, key := range []string{"collection", "table", "collection_name", "title", "name", "id"} {
				if val, ok := rootMap[key].(string); ok && strings.TrimSpace(val) != "" {
					collectionName = strings.TrimSpace(val)
					break
				}
			}
		}
	}

	// Auto-detect collection/model name if not explicitly passed
	if strings.TrimSpace(collectionName) == "" {
		for _, key := range []string{"collection", "table", "collection_name", "title", "name", "id"} {
			if val, ok := rootMap[key].(string); ok && strings.TrimSpace(val) != "" {
				collectionName = strings.TrimSpace(val)
				break
			}
			if val, ok := schemaObj[key].(string); ok && strings.TrimSpace(val) != "" {
				collectionName = strings.TrimSpace(val)
				break
			}
		}
	}
	if strings.TrimSpace(collectionName) == "" {
		collectionName = "imported_collection"
	}

	rootModelID := strings.TrimSpace(collectionName)

	// Parse top-level $defs or definitions
	defs, _ := schemaObj["$defs"].(map[string]any)
	if defs == nil {
		defs, _ = schemaObj["definitions"].(map[string]any)
	}
	if defs == nil {
		defs, _ = rootMap["$defs"].(map[string]any)
	}

	parserCtx := &schemaParserContext{
		createdModels: make(map[string]*model.ModelConfig),
		modelFields:   make(map[string][]*model.DataModel),
		orderedModels: make([]*model.ModelConfig, 0),
		defs:          defs,
		now:           time.Now(),
	}

	rootCfg, rootFields, err := parserCtx.parseObject(rootModelID, schemaObj, false)
	if err != nil {
		return nil, err
	}

	var subModels []*model.ModelConfig
	var subFields []*model.DataModel
	var allModels []*model.ModelConfig
	var allFields []*model.DataModel

	// Order: register reusable submodels first so ValidateCustomType passes when root fields reference them
	for _, m := range parserCtx.orderedModels {
		if m.ID != rootModelID {
			subModels = append(subModels, m)
			allModels = append(allModels, m)
			fields := parserCtx.modelFields[m.ID]
			subFields = append(subFields, fields...)
			allFields = append(allFields, fields...)
		}
	}

	allModels = append(allModels, rootCfg)
	allFields = append(allFields, rootFields...)

	return &SchemaImportResult{
		Model:     rootCfg,
		Fields:    rootFields,
		SubModels: subModels,
		SubFields: subFields,
		AllModels: allModels,
		AllFields: allFields,
	}, nil
}

// parseObject parses an object's properties recursively, creating or reusing ModelConfigs.
func (ctx *schemaParserContext) parseObject(modelID string, schemaObj map[string]any, isSubModel bool) (*model.ModelConfig, []*model.DataModel, error) {
	if schemaObj == nil {
		return nil, nil, fmt.Errorf("schema object cannot be nil")
	}

	props := extractProperties(schemaObj)
	if props == nil {
		return nil, nil, fmt.Errorf("schema does not contain 'properties' or 'property'")
	}

	// 1. Extract property keys, sort ascending
	var keys []string
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// If this is a submodel, determine modelID by joining sorted field names
	if isSubModel {
		if len(keys) == 0 {
			if modelID == "" {
				modelID = fmt.Sprintf("submodel_%d", len(ctx.orderedModels)+1)
			}
		} else {
			modelID = strings.Join(keys, "_")
		}

		// Check if this submodel has already been created (deduplication & reuse!)
		if existingCfg, exists := ctx.createdModels[modelID]; exists {
			return existingCfg, ctx.modelFields[modelID], nil
		}
	}

	// Create ModelConfig
	cfg := &model.ModelConfig{
		ID:                   modelID,
		Name:                 modelID,
		Table:                modelID,
		RefName:              modelID,
		Schema:               "",
		IsTable:              !isSubModel,
		IsAttributeReference: isSubModel,
		Description:          fmt.Sprintf("Imported MongoDB model '%s'", modelID),
		Status:               model.ModelConfigStatusActive,
		Version:              1,
		CreatedAt:            ctx.now,
		UpdatedAt:            ctx.now,
	}
	if isSubModel {
		cfg.Description = fmt.Sprintf("Reusable embedded model (%s)", strings.Join(keys, ", "))
	}

	// Register in context so parallel or circular references reuse it immediately
	ctx.createdModels[modelID] = cfg
	ctx.orderedModels = append(ctx.orderedModels, cfg)

	reqSet := extractRequired(schemaObj)

	var fields []*model.DataModel

	for _, colName := range keys {
		rawProp := props[colName]
		propDef, _ := rawProp.(map[string]any)
		if propDef == nil {
			propDef = make(map[string]any)
		}

		isRequired := reqSet[colName]
		isPK := (colName == "_id" && !isSubModel) // PK for root model _id
		if colName == "_id" {
			isRequired = true
		}

		xType, _ := propDef["x-bsonType"].(string)
		stdType, _ := propDef["type"].(string)

		// ── Check if ARRAY ────────────────────────────────────────────────────────
		itemsDef, hasItems := propDef["items"].(map[string]any)
		isArray := strings.EqualFold(xType, "array") || strings.EqualFold(stdType, "array") || hasItems

		if isArray {
			itemsXType, _ := itemsDef["x-bsonType"].(string)
			itemsStdType, _ := itemsDef["type"].(string)
			itemsProps := extractProperties(itemsDef)

			// Is item inside array an object?
			if strings.EqualFold(itemsXType, "object") || strings.EqualFold(itemsStdType, "object") || len(itemsProps) > 0 {
				subModel, _, err := ctx.parseObject("", itemsDef, true)
				if err != nil {
					return nil, nil, fmt.Errorf("failed parsing array item object for '%s': %w", colName, err)
				}

				subModelID := subModel.ID
				dm := &model.DataModel{
					ID:               fmt.Sprintf("%s_%s", modelID, colName),
					ModelID:          modelID,
					ColumnName:       colName,
					JSONField:        colName,
					DataType:         model.TypeArray,
					PhysicalDataType: "array<object>",
					IsArray:          true,
					CustomTypeID:     &subModelID,
					Items:            &model.ItemRule{Type: model.TypeJSON},
					IsRequired:       isRequired,
					IsNullable:       !isRequired,
					Status:           model.DataModelStatusActive,
					CreatedAt:        ctx.now,
					UpdatedAt:        ctx.now,
				}
				fields = append(fields, dm)
				continue
			}

			// Array of primitives (e.g. location with double)
			itemDataType, itemPhys := resolvePropertyType(itemsDef, ctx.defs)
			phys := fmt.Sprintf("array<%s>", itemPhys)
			dm := &model.DataModel{
				ID:               fmt.Sprintf("%s_%s", modelID, colName),
				ModelID:          modelID,
				ColumnName:       colName,
				JSONField:        colName,
				DataType:         model.TypeArray,
				PhysicalDataType: phys,
				IsArray:          true,
				CustomTypeID:     nil,
				Items:            &model.ItemRule{Type: itemDataType},
				IsRequired:       isRequired,
				IsNullable:       !isRequired,
				Status:           model.DataModelStatusActive,
				CreatedAt:        ctx.now,
				UpdatedAt:        ctx.now,
			}
			fields = append(fields, dm)
			continue
		}

		// ── Check if OBJECT ───────────────────────────────────────────────────────
		childProps := extractProperties(propDef)
		isObject := strings.EqualFold(xType, "object") || strings.EqualFold(stdType, "object") || len(childProps) > 0

		if isObject && len(childProps) > 0 {
			subModel, _, err := ctx.parseObject("", propDef, true)
			if err != nil {
				return nil, nil, fmt.Errorf("failed parsing subobject for '%s': %w", colName, err)
			}

			subModelID := subModel.ID
			dm := &model.DataModel{
				ID:               fmt.Sprintf("%s_%s", modelID, colName),
				ModelID:          modelID,
				ColumnName:       colName,
				JSONField:        colName,
				DataType:         model.TypeJSON,
				PhysicalDataType: "object",
				IsArray:          false,
				CustomTypeID:     &subModelID,
				IsRequired:       isRequired,
				IsNullable:       !isRequired,
				Status:           model.DataModelStatusActive,
				CreatedAt:        ctx.now,
				UpdatedAt:        ctx.now,
			}
			fields = append(fields, dm)
			continue
		}

		// ── PRIMITIVE / SCALAR PROPERTY ───────────────────────────────────────────
		dataType, physType := resolvePropertyType(propDef, ctx.defs)
		dm := &model.DataModel{
			ID:               fmt.Sprintf("%s_%s", modelID, colName),
			ModelID:          modelID,
			ColumnName:       colName,
			JSONField:        colName,
			DataType:         dataType,
			PhysicalDataType: physType,
			IsArray:          false,
			CustomTypeID:     nil,
			IsPrimaryKey:     isPK,
			IsRequired:       isRequired,
			IsNullable:       !isRequired && !isPK,
			Status:           model.DataModelStatusActive,
			CreatedAt:        ctx.now,
			UpdatedAt:        ctx.now,
		}
		fields = append(fields, dm)
	}

	// For root model: ensure _id exists as primary key
	if !isSubModel {
		hasID := false
		for _, dm := range fields {
			if dm.ColumnName == "_id" {
				hasID = true
				dm.IsPrimaryKey = true
				dm.IsRequired = true
				dm.IsNullable = false
				break
			}
		}
		if !hasID {
			idDM := &model.DataModel{
				ID:               fmt.Sprintf("%s__id", modelID),
				ModelID:          modelID,
				ColumnName:       "_id",
				JSONField:        "_id",
				DataType:         model.TypeString,
				PhysicalDataType: "objectId",
				IsPrimaryKey:     true,
				IsRequired:       true,
				IsNullable:       false,
				Status:           model.DataModelStatusActive,
				CreatedAt:        ctx.now,
				UpdatedAt:        ctx.now,
			}
			fields = append([]*model.DataModel{idDM}, fields...)
		}
	}

	// Sort fields: Primary Key first, then alphabetical
	sort.SliceStable(fields, func(i, j int) bool {
		if fields[i].IsPrimaryKey {
			return true
		}
		if fields[j].IsPrimaryKey {
			return false
		}
		return fields[i].ColumnName < fields[j].ColumnName
	})

	ctx.modelFields[modelID] = fields
	return cfg, fields, nil
}

// extractProperties extracts either "properties" or "property" from a schema object.
func extractProperties(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	if p, ok := m["properties"].(map[string]any); ok && p != nil {
		return p
	}
	if p, ok := m["property"].(map[string]any); ok && p != nil {
		return p
	}
	return nil
}

// extractRequired extracts required field names into a fast lookup map.
func extractRequired(m map[string]any) map[string]bool {
	set := make(map[string]bool)
	if m == nil {
		return set
	}
	if arr, ok := m["required"].([]any); ok {
		for _, r := range arr {
			if s, ok := r.(string); ok && strings.TrimSpace(s) != "" {
				set[strings.TrimSpace(s)] = true
			}
		}
	} else if arrStr, ok := m["required"].([]string); ok {
		for _, s := range arrStr {
			if strings.TrimSpace(s) != "" {
				set[strings.TrimSpace(s)] = true
			}
		}
	}
	return set
}

// resolvePropertyType maps JSON Schema and BSON type notations to model.DataType and physical representation.
func resolvePropertyType(prop map[string]any, defs map[string]any) (model.DataType, string) {
	if prop == nil {
		return model.TypeString, "string"
	}

	// 1. Direct x-bsonType (MongoDB Compass / Atlas standard)
	if bsonType, ok := prop["x-bsonType"].(string); ok && strings.TrimSpace(bsonType) != "" {
		switch strings.ToLower(strings.TrimSpace(bsonType)) {
		case "string":
			return model.TypeString, "string"
		case "objectid":
			return model.TypeString, "objectId"
		case "double":
			return model.TypeDecimal, "double"
		case "decimal", "decimal128":
			return model.TypeDecimal, "decimal128"
		case "int", "int32":
			return model.TypeInt, "int"
		case "long", "int64":
			return model.TypeLong, "long"
		case "bool", "boolean":
			return model.TypeBoolean, "bool"
		case "date":
			return model.TypeDateTime, "date"
		case "timestamp":
			return model.TypeDateTime, "timestamp"
		case "bindata", "binary":
			return model.TypeBinary, "binData"
		case "regex":
			return model.TypeString, "regex"
		case "array":
			return model.TypeArray, "array"
		case "object":
			return model.TypeJSON, "object"
		case "null":
			return model.TypeString, "null"
		}
	}

	// 2. $ref (e.g. #/$defs/Double, #/$defs/Date)
	if ref, ok := prop["$ref"].(string); ok && strings.TrimSpace(ref) != "" {
		refLower := strings.ToLower(ref)
		if strings.Contains(refLower, "double") || strings.Contains(refLower, "decimal") {
			return model.TypeDecimal, "double"
		}
		if strings.Contains(refLower, "date") || strings.Contains(refLower, "timestamp") {
			return model.TypeDateTime, "date"
		}
		if strings.Contains(refLower, "long") || strings.Contains(refLower, "int64") {
			return model.TypeLong, "long"
		}
		if strings.Contains(refLower, "int") || strings.Contains(refLower, "int32") {
			return model.TypeInt, "int"
		}
		if strings.Contains(refLower, "objectid") {
			return model.TypeString, "objectId"
		}
	}

	// 3. JSON Schema standard type
	if typ, ok := prop["type"].(string); ok && strings.TrimSpace(typ) != "" {
		switch strings.ToLower(strings.TrimSpace(typ)) {
		case "string":
			if fmtStr, ok := prop["format"].(string); ok {
				if fmtStr == "date-time" || fmtStr == "date" {
					return model.TypeDateTime, "date"
				}
			}
			return model.TypeString, "string"
		case "number":
			return model.TypeDecimal, "double"
		case "integer":
			return model.TypeInt, "int"
		case "boolean":
			return model.TypeBoolean, "bool"
		case "array":
			return model.TypeArray, "array"
		case "object":
			return model.TypeJSON, "object"
		}
	}

	return model.TypeString, "string"
}
