package mongodb

import (
	"context"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/model"
)

func TestParseMongoJSONSchema_CompassFormat(t *testing.T) {
	rawJSON := `{
  "type": "object",
  "x-bsonType": "object",
  "required": [
    "_id",
    "amount",
    "bill_image",
    "bill_number",
    "created_by",
    "created_on",
    "date",
    "shop_id",
    "status",
    "user_id"
  ],
  "properties": {
    "_id": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": {
        "hasDuplicates": false,
        "probability": 1,
        "count": 12
      },
      "x-sampleValues": [
        "#CLM-ZW61VU",
        "#CLM-MC5HPZ"
      ]
    },
    "amount": {
      "$ref": "#/$defs/Double",
      "x-bsonType": "double",
      "x-metadata": {
        "hasDuplicates": true,
        "probability": 1,
        "count": 12
      },
      "x-sampleValues": [ 500, 300 ]
    },
    "bill_image": {
      "type": "object",
      "x-bsonType": "object",
      "required": [
        "_id",
        "file_name",
        "ref_id",
        "size",
        "storage_name",
        "uploaded_by",
        "uploaded_on"
      ],
      "properties": {
        "_id": {
          "type": "string",
          "x-bsonType": "string"
        },
        "file_name": {
          "type": "string",
          "x-bsonType": "string"
        },
        "ref_id": {
          "type": "string",
          "x-bsonType": "string"
        },
        "size": {
          "$ref": "#/$defs/Double",
          "x-bsonType": "double"
        },
        "storage_name": {
          "type": "string",
          "x-bsonType": "string"
        },
        "uploaded_by": {
          "type": "string",
          "x-bsonType": "string"
        },
        "uploaded_on": {
          "type": "string",
          "x-bsonType": "string"
        }
      }
    },
    "bill_number": {
      "type": "string",
      "x-bsonType": "string"
    },
    "created_by": {
      "type": "string",
      "x-bsonType": "string"
    },
    "created_on": {
      "$ref": "#/$defs/Date",
      "x-bsonType": "date"
    },
    "date": {
      "$ref": "#/$defs/Date",
      "x-bsonType": "date"
    },
    "points": {
      "$ref": "#/$defs/Double",
      "x-bsonType": "double"
    },
    "reason": {
      "type": "string",
      "x-bsonType": "string"
    },
    "shop_id": {
      "type": "string",
      "x-bsonType": "string"
    },
    "status": {
      "type": "string",
      "x-bsonType": "string"
    },
    "update_by": {
      "type": "string",
      "x-bsonType": "string"
    },
    "update_on": {
      "$ref": "#/$defs/Date",
      "x-bsonType": "date"
    },
    "user_id": {
      "type": "string",
      "x-bsonType": "string"
    }
  },
  "$defs": {
    "Double": {
      "oneOf": [
        { "type": "number" },
        { "type": "object", "properties": { "$numberDouble": { "enum": [ "Infinity", "-Infinity", "NaN" ] } } }
      ]
    },
    "Date": {
      "type": "object",
      "properties": { "$date": { "type": "string", "format": "date-time" } },
      "required": [ "$date" ],
      "additionalProperties": false
    }
  }
}`

	res, err := ParseMongoJSONSchema("claims", rawJSON)
	if err != nil {
		t.Fatalf("unexpected error parsing json schema: %v", err)
	}

	if res == nil || res.Model == nil {
		t.Fatal("expected non-nil SchemaImportResult and ModelConfig")
	}
	if res.Model.ID != "claims" || res.Model.Table != "claims" {
		t.Errorf("expected ModelConfig table 'claims', got '%s'", res.Model.Table)
	}

	fieldMap := make(map[string]*model.DataModel)
	for _, f := range res.Fields {
		fieldMap[f.ColumnName] = f
	}

	// 1. Check Primary Key _id
	idField, ok := fieldMap["_id"]
	if !ok {
		t.Fatal("missing _id field")
	}
	if !idField.IsPrimaryKey {
		t.Errorf("expected _id to be primary key")
	}
	if !idField.IsRequired || idField.IsNullable {
		t.Errorf("expected _id to be required and not nullable")
	}

	// 2. Check amount (double from $ref / x-bsonType)
	amtField, ok := fieldMap["amount"]
	if !ok {
		t.Fatal("missing amount field")
	}
	if amtField.DataType != model.TypeDecimal {
		t.Errorf("expected amount data type Decimal, got %s", amtField.DataType)
	}
	if !amtField.IsRequired || amtField.IsNullable {
		t.Errorf("expected amount to be required and non-nullable")
	}

	// 3. Check bill_image object & separate reusable submodel
	imgField, ok := fieldMap["bill_image"]
	if !ok {
		t.Fatal("missing bill_image parent field")
	}
	if imgField.DataType != model.TypeJSON {
		t.Errorf("expected bill_image to be TypeJSON, got %s", imgField.DataType)
	}
	expectedSubModelID := "_id_file_name_ref_id_size_storage_name_uploaded_by_uploaded_on"
	if imgField.CustomTypeID == nil || *imgField.CustomTypeID != expectedSubModelID {
		t.Errorf("expected bill_image CustomTypeID '%s', got '%v'", expectedSubModelID, imgField.CustomTypeID)
	}

	// Check submodel was created
	if len(res.SubModels) != 1 {
		t.Fatalf("expected 1 submodel, got %d", len(res.SubModels))
	}
	if res.SubModels[0].ID != expectedSubModelID {
		t.Errorf("expected submodel ID '%s', got '%s'", expectedSubModelID, res.SubModels[0].ID)
	}
	if !res.SubModels[0].IsAttributeReference {
		t.Errorf("expected submodel to have IsAttributeReference = true")
	}

	// Check submodel fields
	if len(res.SubFields) != 7 {
		t.Errorf("expected 7 fields in submodel, got %d", len(res.SubFields))
	}

	// 4. Check created_on Date
	createdOn, ok := fieldMap["created_on"]
	if !ok {
		t.Fatal("missing created_on field")
	}
	if createdOn.DataType != model.TypeDateTime {
		t.Errorf("expected created_on to be DateTime, got %s", createdOn.DataType)
	}

	// 5. Check nullable fields (not in required: points, reason, update_by, update_on)
	points, ok := fieldMap["points"]
	if !ok {
		t.Fatal("missing points field")
	}
	if points.IsRequired || !points.IsNullable {
		t.Errorf("expected points to be nullable and NOT required")
	}

	// 6. Verify default values are NOT populated
	for _, f := range res.AllFields {
		if f.DefaultValue != nil {
			t.Errorf("expected DefaultValue to be nil for field '%s', got '%v'", f.ColumnName, f.DefaultValue)
		}
	}

	t.Logf("Successfully verified %d root fields and %d submodel fields for model '%s'", len(res.Fields), len(res.SubFields), res.Model.ID)
}

func TestMongoAdapter_ImportJSONSchema(t *testing.T) {
	adapter := NewMongoAdapter("mongodb://localhost:27017", "test_db")
	ctx := context.Background()

	wrapperJSON := map[string]any{
		"collection": "test_orders",
		"schema": map[string]any{
			"type":     "object",
			"required": []any{"_id", "order_total"},
			"properties": map[string]any{
				"_id":         map[string]any{"x-bsonType": "objectId"},
				"order_total": map[string]any{"x-bsonType": "double"},
				"notes":       map[string]any{"type": "string"},
			},
		},
	}

	res, err := adapter.ImportJSONSchema(ctx, "", wrapperJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Model.Table != "test_orders" {
		t.Errorf("expected table 'test_orders', got '%s'", res.Model.Table)
	}
	if len(res.Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(res.Fields))
	}
}

func TestParseMongoJSONSchema_ShopsFullSchema(t *testing.T) {
	rawJSON := `{
  "type": "object",
  "x-bsonType": "object",
  "required": [
    "_id",
    "contact_name",
    "contact_number",
    "createdAt",
    "delegates",
    "details",
    "enable_referral_bonus",
    "isShopDetailsComplete",
    "location",
    "main_category",
    "name",
    "rating",
    "referral_points",
    "return_points_percent",
    "review_count",
    "shop_image",
    "shop_location",
    "slabs",
    "street_view_image",
    "sub_category",
    "timings",
    "total_views",
    "update_by",
    "update_on",
    "updatedAt"
  ],
  "properties": {
    "_id": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "41b79f4226ff4e9d9ce0fd328db34efb", "7d55a0c69cfb48a6859e2dad5839876b" ]
    },
    "contact_name": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "Manikandan S", "Umapathi S" ]
    },
    "contact_number": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "9875463210", "7094350137" ]
    },
    "createdAt": {
      "$ref": "#/$defs/Date",
      "x-bsonType": "date",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "2026-05-15T06:03:29.263Z", "2026-05-15T05:55:12.720Z" ]
    },
    "delegates": {
      "type": "array",
      "x-bsonType": "array",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "items": {
        "type": "object",
        "x-bsonType": "object",
        "x-metadata": { "probability": 1, "count": 2 },
        "required": [ "time", "user_id" ],
        "properties": {
          "time": {
            "$ref": "#/$defs/Date",
            "x-bsonType": "date",
            "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
            "x-sampleValues": [ "2026-05-15T06:03:29.263Z", "2026-05-15T05:55:12.738Z" ]
          },
          "user_id": {
            "type": "string",
            "x-bsonType": "string",
            "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
            "x-sampleValues": [ "e7ec3dd3377442478d381438be1e94df", "72ca534a1ed64ff78beb73268dfdae98" ]
          }
        }
      }
    },
    "details": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "Reliance Digital", "H&M" ]
    },
    "enable_referral_bonus": {
      "type": "boolean",
      "x-bsonType": "bool",
      "x-metadata": { "hasDuplicates": true, "probability": 1, "count": 2 },
      "x-sampleValues": [ true, true ]
    },
    "isShopDetailsComplete": {
      "type": "boolean",
      "x-bsonType": "bool",
      "x-metadata": { "hasDuplicates": true, "probability": 1, "count": 2 },
      "x-sampleValues": [ true, true ]
    },
    "location": {
      "type": "array",
      "x-bsonType": "array",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "items": {
        "$ref": "#/$defs/Double",
        "x-bsonType": "double",
        "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 4 },
        "x-sampleValues": [ 80.2504224, 13.040776 ]
      }
    },
    "main_category": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "electronics", "fashion" ]
    },
    "name": {
      "type": "string",
      "x-bsonType": "string",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ "Reliance Digital", "H&M" ]
    },
    "rating": {
      "$ref": "#/$defs/Double",
      "x-bsonType": "double",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ 4.5, 3 ]
    },
    "referral_points": {
      "$ref": "#/$defs/Double",
      "x-bsonType": "double",
      "x-metadata": { "hasDuplicates": true, "probability": 1, "count": 2 },
      "x-sampleValues": [ 20, 20 ]
    },
    "return_points_percent": {
      "$ref": "#/$defs/Double",
      "x-bsonType": "double",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "x-sampleValues": [ 20, 10 ]
    },
    "review_count": {
      "type": "integer",
      "x-bsonType": "int",
      "x-metadata": { "hasDuplicates": true, "probability": 1, "count": 2 },
      "x-sampleValues": [ 2, 2 ]
    },
    "shop_image": {
      "type": "object",
      "x-bsonType": "object",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "required": [ "_id", "file_name", "ref_id", "size", "storage_name", "uploaded_by", "uploaded_on" ],
      "properties": {
        "_id": { "type": "string", "x-bsonType": "string" },
        "file_name": { "type": "string", "x-bsonType": "string" },
        "ref_id": { "type": "string", "x-bsonType": "string" },
        "size": { "$ref": "#/$defs/Double", "x-bsonType": "double" },
        "storage_name": { "type": "string", "x-bsonType": "string" },
        "uploaded_by": { "type": "string", "x-bsonType": "string" },
        "uploaded_on": { "type": "string", "x-bsonType": "string" }
      }
    },
    "shop_location": {
      "type": "object",
      "x-bsonType": "object",
      "x-metadata": { "hasDuplicates": false, "probability": 1, "count": 2 },
      "required": [ "lat", "lng" ],
      "properties": {
        "lat": { "$ref": "#/$defs/Double", "x-bsonType": "double" },
        "lng": { "$ref": "#/$defs/Double", "x-bsonType": "double" }
      }
    },
    "slabs": {
      "type": "array",
      "x-bsonType": "array",
      "items": {
        "type": "object",
        "x-bsonType": "object",
        "required": [ "max", "min", "money", "points" ],
        "properties": {
          "max": { "$ref": "#/$defs/Double", "x-bsonType": "double" },
          "min": { "$ref": "#/$defs/Double", "x-bsonType": "double" },
          "money": { "$ref": "#/$defs/Double", "x-bsonType": "double" },
          "points": { "$ref": "#/$defs/Double", "x-bsonType": "double" }
        }
      }
    },
    "street_view_image": {
      "type": "object",
      "x-bsonType": "object",
      "required": [ "_id", "file_name", "ref_id", "size", "storage_name", "uploaded_by", "uploaded_on" ],
      "properties": {
        "_id": { "type": "string", "x-bsonType": "string" },
        "file_name": { "type": "string", "x-bsonType": "string" },
        "ref_id": { "type": "string", "x-bsonType": "string" },
        "size": { "$ref": "#/$defs/Double", "x-bsonType": "double" },
        "storage_name": { "type": "string", "x-bsonType": "string" },
        "uploaded_by": { "type": "string", "x-bsonType": "string" },
        "uploaded_on": { "type": "string", "x-bsonType": "string" }
      }
    },
    "sub_category": {
      "type": "string",
      "x-bsonType": "string"
    },
    "timings": {
      "type": "array",
      "x-bsonType": "array",
      "items": {
        "type": "object",
        "x-bsonType": "object",
        "required": [ "day" ],
        "properties": {
          "closed": { "type": "boolean", "x-bsonType": "bool" },
          "closing": { "type": "string", "x-bsonType": "string" },
          "day": { "type": "string", "x-bsonType": "string" },
          "opening": { "type": "string", "x-bsonType": "string" }
        }
      }
    },
    "total_views": {
      "type": "integer",
      "x-bsonType": "int"
    },
    "update_by": {
      "type": "string",
      "x-bsonType": "string"
    },
    "update_on": {
      "$ref": "#/$defs/Date",
      "x-bsonType": "date"
    },
    "updatedAt": {
      "$ref": "#/$defs/Date",
      "x-bsonType": "date"
    }
  },
  "$defs": {
    "Double": { "oneOf": [ { "type": "number" } ] },
    "Date": { "type": "object", "properties": { "$date": { "type": "string" } } }
  }
}`

	res, err := ParseMongoJSONSchema("shops", rawJSON)
	if err != nil {
		t.Fatalf("unexpected error parsing shops schema: %v", err)
	}

	if res.Model.ID != "shops" {
		t.Errorf("expected ID 'shops', got '%s'", res.Model.ID)
	}

	// 1. Verify 25 root fields
	if len(res.Fields) != 25 {
		t.Errorf("expected 25 root fields, got %d", len(res.Fields))
	}

	fieldMap := make(map[string]*model.DataModel)
	for _, f := range res.Fields {
		fieldMap[f.ColumnName] = f
	}

	// 2. Verify location is an array of double primitive
	loc, ok := fieldMap["location"]
	if !ok {
		t.Fatal("missing location field")
	}
	if !loc.IsArray || loc.DataType != model.TypeArray || loc.PhysicalDataType != "array<double>" {
		t.Errorf("expected location array<double>, got DataType=%s, Phys=%s, isArray=%t", loc.DataType, loc.PhysicalDataType, loc.IsArray)
	}
	if loc.CustomTypeID != nil {
		t.Errorf("expected location CustomTypeID to be nil, got '%v'", loc.CustomTypeID)
	}

	// 3. Verify delegates array of objects -> links to "time_user_id"
	delegates, ok := fieldMap["delegates"]
	if !ok {
		t.Fatal("missing delegates field")
	}
	if !delegates.IsArray || delegates.DataType != model.TypeArray || delegates.PhysicalDataType != "array<object>" {
		t.Errorf("expected delegates array<object>, got %s (%s)", delegates.DataType, delegates.PhysicalDataType)
	}
	if delegates.CustomTypeID == nil || *delegates.CustomTypeID != "time_user_id" {
		t.Errorf("expected delegates CustomTypeID 'time_user_id', got '%v'", delegates.CustomTypeID)
	}

	// 4. Verify shop_image & street_view_image REUSE the EXACT same submodel!
	imageSubModelID := "_id_file_name_ref_id_size_storage_name_uploaded_by_uploaded_on"
	shopImg, ok := fieldMap["shop_image"]
	if !ok {
		t.Fatal("missing shop_image field")
	}
	if shopImg.CustomTypeID == nil || *shopImg.CustomTypeID != imageSubModelID {
		t.Errorf("expected shop_image CustomTypeID '%s', got '%v'", imageSubModelID, shopImg.CustomTypeID)
	}

	streetImg, ok := fieldMap["street_view_image"]
	if !ok {
		t.Fatal("missing street_view_image field")
	}
	if streetImg.CustomTypeID == nil || *streetImg.CustomTypeID != imageSubModelID {
		t.Errorf("expected street_view_image CustomTypeID '%s', got '%v'", imageSubModelID, streetImg.CustomTypeID)
	}

	// Deduplication test: count how many times imageSubModelID was created in SubModels
	imageModelCount := 0
	for _, sm := range res.SubModels {
		if sm.ID == imageSubModelID {
			imageModelCount++
		}
	}
	if imageModelCount != 1 {
		t.Errorf("expected imageSubModelID to be created exactly ONCE and reused, got count=%d", imageModelCount)
	}

	// 5. Verify slabs array of objects -> links to "max_min_money_points"
	slabs, ok := fieldMap["slabs"]
	if !ok {
		t.Fatal("missing slabs field")
	}
	if slabs.CustomTypeID == nil || *slabs.CustomTypeID != "max_min_money_points" {
		t.Errorf("expected slabs CustomTypeID 'max_min_money_points', got '%v'", slabs.CustomTypeID)
	}

	// 6. Verify timings array of objects -> links to "closed_closing_day_opening"
	timings, ok := fieldMap["timings"]
	if !ok {
		t.Fatal("missing timings field")
	}
	if timings.CustomTypeID == nil || *timings.CustomTypeID != "closed_closing_day_opening" {
		t.Errorf("expected timings CustomTypeID 'closed_closing_day_opening', got '%v'", timings.CustomTypeID)
	}

	// 7. Verify shop_location subobject -> links to "lat_lng"
	shopLoc, ok := fieldMap["shop_location"]
	if !ok {
		t.Fatal("missing shop_location field")
	}
	if shopLoc.CustomTypeID == nil || *shopLoc.CustomTypeID != "lat_lng" {
		t.Errorf("expected shop_location CustomTypeID 'lat_lng', got '%v'", shopLoc.CustomTypeID)
	}

	// 8. Total submodels: 5 (time_user_id, image, lat_lng, slabs, timings)
	if len(res.SubModels) != 5 {
		t.Errorf("expected 5 submodels, got %d", len(res.SubModels))
	}

	// 9. Verify no default values are set across any fields
	for _, f := range res.AllFields {
		if f.DefaultValue != nil {
			t.Errorf("expected nil DefaultValue on field '%s.%s', got '%v'", f.ModelID, f.ColumnName, f.DefaultValue)
		}
	}

	t.Logf("Successfully verified %d root fields and %d submodels (total %d fields) for 'shops'", len(res.Fields), len(res.SubModels), len(res.AllFields))
}
