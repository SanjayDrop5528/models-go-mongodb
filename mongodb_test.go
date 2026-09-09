package mongodb_test

import (
	"context"
	"github.com/SanjayDrop5528/models-go-mongodb"
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

	filter := qb.BuildFilter(q)
	if filter["name"] != "Alice" {
		t.Fatalf("expected name Alice, got %v", filter["name"])
	}
	if gt, ok := filter["age"].(map[string]any); !ok || gt["$gt"] != 30 {
		t.Fatalf("expected age > 30, got %v", filter["age"])
	}
}

func TestMongo_UnifiedQueryPipeline(t *testing.T) {
	qb := &mongodb.QueryBuilder{}

	q := query.New().
		Table("users").
		Where("status = ?", "active").
		Column("id", "name", "email").
		ExcludeColumn("password").
		Relation("Profile").
		OrderBy("created_at", query.SortDesc).
		Limit(10).
		Offset(5)

	pipeline := qb.BuildPipeline(q)

	if len(pipeline) < 5 {
		t.Fatalf("expected at least 5 pipeline stages, got %d", len(pipeline))
	}

	hasMatch := false
	hasLookup := false
	hasProject := false
	hasSort := false
	hasSkip := false
	hasLimit := false

	for _, stage := range pipeline {
		if _, ok := stage["$match"]; ok {
			hasMatch = true
		}
		if _, ok := stage["$lookup"]; ok {
			hasLookup = true
		}
		if _, ok := stage["$project"]; ok {
			hasProject = true
		}
		if _, ok := stage["$sort"]; ok {
			hasSort = true
		}
		if _, ok := stage["$skip"]; ok {
			hasSkip = true
		}
		if _, ok := stage["$limit"]; ok {
			hasLimit = true
		}
	}

	if !hasMatch || !hasLookup || !hasProject || !hasSort || !hasSkip || !hasLimit {
		t.Fatalf("missing pipeline stage: match=%v, lookup=%v, project=%v, sort=%v, skip=%v, limit=%v",
			hasMatch, hasLookup, hasProject, hasSort, hasSkip, hasLimit)
	}
}
