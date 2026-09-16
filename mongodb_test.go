package mongodb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
	"github.com/SanjayDrop5528/models-go-engine/dataset/resolver"
	"github.com/SanjayDrop5528/models-go-engine/diff"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/plan"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"github.com/SanjayDrop5528/models-go-mongodb"
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

func TestMongoDataSetCompiler_AllCustomAndAggregateFunctions(t *testing.T) {
	c := mongodb.NewMongoDataSetCompiler()

	// 1. Test Math, String, Date, and Conditional Row Calculations
	dsCalc := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "orders"},
		CustomColumns: []domain.CustomColumn{
			{CustomColumnName: "col_add", CustomAggregateFnName: "ADD", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "subtotal"}, {TableName: "_LITERAL_", FieldName: "10", IsLiteral: true}}},
			{CustomColumnName: "col_sub", CustomAggregateFnName: "SUBTRACT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "total"}, {TableName: "orders", FieldName: "tax"}}},
			{CustomColumnName: "col_mul", CustomAggregateFnName: "MULTIPLY", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "price"}, {TableName: "orders", FieldName: "qty"}}},
			{CustomColumnName: "col_div", CustomAggregateFnName: "DIVIDE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "total"}, {TableName: "orders", FieldName: "items_count"}}},
			{CustomColumnName: "col_mod", CustomAggregateFnName: "MODULO", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "id"}, {TableName: "_LITERAL_", FieldName: "10", IsLiteral: true}}},
			{CustomColumnName: "col_pow", CustomAggregateFnName: "POWER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "rating"}, {TableName: "_LITERAL_", FieldName: "2", IsLiteral: true}}},
			{CustomColumnName: "col_round", CustomAggregateFnName: "ROUND", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}, {TableName: "_LITERAL_", FieldName: "2", IsLiteral: true}}},
			{CustomColumnName: "col_ceil", CustomAggregateFnName: "CEIL", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "shipping_fee"}}},
			{CustomColumnName: "col_floor", CustomAggregateFnName: "FLOOR", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "shipping_fee"}}},
			{CustomColumnName: "col_abs", CustomAggregateFnName: "ABS", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "variance"}}},
			{CustomColumnName: "col_sqrt", CustomAggregateFnName: "SQRT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "area"}}},

			{CustomColumnName: "col_concat", CustomAggregateFnName: "CONCAT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "prefix"}, {TableName: "_LITERAL_", FieldName: "-", IsLiteral: true}, {TableName: "orders", FieldName: "order_num"}}},
			{CustomColumnName: "col_concat_ws", CustomAggregateFnName: "CONCAT_WS", Fields: []domain.DataSetCustomField{{TableName: "_LITERAL_", FieldName: ",", IsLiteral: true}, {TableName: "orders", FieldName: "city"}, {TableName: "orders", FieldName: "country"}}},
			{CustomColumnName: "col_upper", CustomAggregateFnName: "UPPER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "code"}}},
			{CustomColumnName: "col_lower", CustomAggregateFnName: "LOWER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "email"}}},
			{CustomColumnName: "col_trim", CustomAggregateFnName: "TRIM", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "notes"}}},
			{CustomColumnName: "col_len", CustomAggregateFnName: "LENGTH", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "code"}}},
			{CustomColumnName: "col_substr", CustomAggregateFnName: "SUBSTRING", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "code"}, {TableName: "_LITERAL_", FieldName: "1", IsLiteral: true}, {TableName: "_LITERAL_", FieldName: "4", IsLiteral: true}}},
			{CustomColumnName: "col_replace", CustomAggregateFnName: "REPLACE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "title"}, {TableName: "_LITERAL_", FieldName: "old", IsLiteral: true}, {TableName: "_LITERAL_", FieldName: "new", IsLiteral: true}}},

			{CustomColumnName: "col_year", CustomAggregateFnName: "YEAR", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}}},
			{CustomColumnName: "col_month", CustomAggregateFnName: "MONTH", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}}},
			{CustomColumnName: "col_day", CustomAggregateFnName: "DAY", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}}},
			{CustomColumnName: "col_now", CustomAggregateFnName: "NOW"},
			{CustomColumnName: "col_today", CustomAggregateFnName: "CURRENT_DATE"},
			{CustomColumnName: "col_date_diff", CustomAggregateFnName: "DATE_DIFF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "delivered_at"}, {TableName: "orders", FieldName: "shipped_at"}}},
			{CustomColumnName: "col_date_add", CustomAggregateFnName: "DATE_ADD", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "order_date"}, {TableName: "_LITERAL_", FieldName: "7", IsLiteral: true}}},

			{CustomColumnName: "col_pct", CustomAggregateFnName: "PERCENTAGE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "margin"}, {TableName: "orders", FieldName: "revenue"}}},
			{CustomColumnName: "col_disc", CustomAggregateFnName: "DISCOUNT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "price"}, {TableName: "_LITERAL_", FieldName: "15", IsLiteral: true}}},
			{CustomColumnName: "col_coalesce", CustomAggregateFnName: "COALESCE", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "discount"}, {TableName: "_LITERAL_", FieldName: "0", IsLiteral: true}}},
			{CustomColumnName: "col_if", CustomAggregateFnName: "IF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "is_gift"}, {TableName: "_LITERAL_", FieldName: "5", IsLiteral: true}, {TableName: "_LITERAL_", FieldName: "0", IsLiteral: true}}},
			{CustomColumnName: "col_to_str", CustomAggregateFnName: "TO_STRING", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "id"}}},
			{CustomColumnName: "col_to_int", CustomAggregateFnName: "TO_INTEGER", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "col_to_dec", CustomAggregateFnName: "TO_DECIMAL", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "rate"}}},
		},
	}

	astPlanner := planner.NewPlanner(resolver.NewFunctionRegistry())
	astCalc, err := astPlanner.BuildAST(context.Background(), dsCalc)
	if err != nil {
		t.Fatalf("failed building calculation AST: %v", err)
	}

	resCalc, err := c.Compile(context.Background(), astCalc, dsCalc)
	if err != nil {
		t.Fatalf("failed compiling calculation pipeline: %v", err)
	}

	expectedSnippets := []string{
		`"$add"`,
		`"$subtract"`,
		`"$multiply"`,
		`"$divide"`,
		`"$mod"`,
		`"$pow"`,
		`"$round"`,
		`"$ceil"`,
		`"$floor"`,
		`"$abs"`,
		`"$sqrt"`,
		`"$concat"`,
		`"$toUpper"`,
		`"$toLower"`,
		`"$trim"`,
		`"$strLenCP"`,
		`"$substrCP"`,
		`"$replaceAll"`,
		`"$year"`,
		`"$month"`,
		`"$dayOfMonth"`,
		`"$$NOW"`,
		`"$dateDiff"`,
		`"$dateAdd"`,
		`"$ifNull"`,
		`"$cond"`,
		`"$toString"`,
		`"$toInt"`,
		`"$toDecimal"`,
	}

	for _, snippet := range expectedSnippets {
		if !strings.Contains(resCalc.ExecutableQuery, snippet) {
			t.Errorf("missing expected snippet:\n  %s\nin compiled pipeline:\n  %s", snippet, resCalc.ExecutableQuery)
		}
	}

	// 2. Test Aggregates (SUM, AVG, MIN, MAX, COUNT, COUNT_ALL, COUNT_DISTINCT, COUNT_IF, SUM_IF)
	dsAgg := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "orders"},
		GroupByFields: []domain.GroupByField{
			{TableName: "orders", FieldName: "status"},
		},
		CustomColumns: []domain.CustomColumn{
			{CustomColumnName: "total_amount", CustomAggregateFnName: "SUM", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "avg_amount", CustomAggregateFnName: "AVG", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "min_amount", CustomAggregateFnName: "MIN", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "max_amount", CustomAggregateFnName: "MAX", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "amount"}}},
			{CustomColumnName: "order_count", CustomAggregateFnName: "COUNT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "id"}}},
			{CustomColumnName: "row_count", CustomAggregateFnName: "COUNT_ALL"},
			{CustomColumnName: "distinct_customers", CustomAggregateFnName: "COUNT_DISTINCT", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "customer_id"}}},
			{CustomColumnName: "delivered_count", CustomAggregateFnName: "COUNT_IF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "is_delivered"}}},
			{CustomColumnName: "active_total", CustomAggregateFnName: "SUM_IF", Fields: []domain.DataSetCustomField{{TableName: "orders", FieldName: "is_active"}, {TableName: "orders", FieldName: "amount"}}},
		},
	}

	astAgg, err := astPlanner.BuildAST(context.Background(), dsAgg)
	if err != nil {
		t.Fatalf("failed building aggregate AST: %v", err)
	}

	resAgg, err := c.Compile(context.Background(), astAgg, dsAgg)
	if err != nil {
		t.Fatalf("failed compiling aggregate pipeline: %v", err)
	}

	expectedAggSnippets := []string{
		`"total_amount":{"$sum":"$amount"}`,
		`"avg_amount":{"$avg":"$amount"}`,
		`"min_amount":{"$min":"$amount"}`,
		`"max_amount":{"$max":"$amount"}`,
		`"row_count":{"$sum":1}`,
		`"distinct_customers_set":{"$addToSet":"$customer_id"}`,
		`"distinct_customers":{"$size":"$distinct_customers_set"}`,
		`"delivered_count":{"$sum":{"$cond":["$is_delivered",1,0]}}`,
		`"active_total":{"$sum":{"$cond":["$is_active","$amount",0]}}`,
		`"_id":"$status"`,
	}

	for _, snippet := range expectedAggSnippets {
		if !strings.Contains(resAgg.ExecutableQuery, snippet) {
			t.Errorf("missing expected aggregate snippet:\n  %s\nin compiled pipeline:\n  %s", snippet, resAgg.ExecutableQuery)
		}
	}
}

