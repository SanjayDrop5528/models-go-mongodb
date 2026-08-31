package mongodb_test

import (
	"context"
	"models-go/adapters/mongodb"
	"github.com/SanjayDrop5528/models-go-engine/diff"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/plan"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"strings"
	"testing"
)

func TestMongo_Preview_DynamicFieldAddition(t *testing.T) {
	adapter := mongodb.NewMongoAdapter("", "testdb")

	p := &plan.SchemaPlan{
		ModelID:     "employee",
		StorageName: "employees",
		Database:    "mongodb",
		Operations: []diff.SchemaOperation{
			{
				Type:        diff.OpAddColumn,
				TargetTable: "employees",
				ObjectName:  "salary",
				After: schema.SchemaAttribute{
					Name: "salary",
					Type: model.TypeDecimal,
				},
				Safety:      diff.SafetySafe,
				Destructive: false,
			},
		},
	}

	preview, err := adapter.PreviewSchemaChange(context.Background(), p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(preview.NativeActions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(preview.NativeActions))
	}

	action := preview.NativeActions[0]
	if action.Type != "MONGODB_DYNAMIC_FIELD" {
		t.Fatalf("expected MONGODB_DYNAMIC_FIELD, got %s", action.Type)
	}
	if !strings.Contains(action.Description, "Dynamic field 'salary' accepted without physical table alteration") {
		t.Fatalf("expected dynamic schema description, got %s", action.Description)
	}
}

func TestMongo_QueryFilter(t *testing.T) {
	qb := &mongodb.QueryBuilder{}

	q := query.NewQuery().
		Where("age", query.OpGt, 30).
		Where("name", query.OpEq, "Alice")

	filterDoc := qb.BuildFilter(q)

	if filterDoc["name"] != "Alice" {
		t.Fatalf("expected name Alice, got %v", filterDoc["name"])
	}
	ageCond, ok := filterDoc["age"].(map[string]any)
	if !ok || ageCond["$gt"] != 30 {
		t.Fatalf("expected age $gt 30, got %v", filterDoc["age"])
	}
}
