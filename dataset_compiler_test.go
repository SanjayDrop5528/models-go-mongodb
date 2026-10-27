package mongodb_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
	"github.com/SanjayDrop5528/models-go-mongodb"
)

func TestMongoDataSetCompiler_BaseFilterAndParams(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "employees",
			Alias: "employees",
			Filter: map[string]any{
				"is_deleted": false,
			},
		},
		WhereFilters: []planner.ASTCondition{
			{
				Table:         "employees",
				Column:        "status",
				Operator:      "=",
				IsParamRef:    true,
				ParamName:     "status_param",
				ParamDataType: "TEXT",
			},
		},
		Parameters: []domain.FilterParam{
			{
				ParamName:     "status_param",
				ParamDataType: "TEXT",
				DefaultValue:  "active",
			},
		},
		Projections: []planner.ASTProjection{
			{
				SourceTable: "employees",
				SourceField: "name",
				Alias:       "name",
			},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}

	// 1. Executable pipeline (default values substituted)
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	var execStages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &execStages); err != nil {
		t.Fatalf("failed to parse executable pipeline: %v", err)
	}

	if len(execStages) < 2 {
		t.Fatalf("expected at least 2 stages ($match, $project), got %d", len(execStages))
	}

	matchStage, ok := execStages[0]["$match"].(map[string]any)
	if !ok {
		t.Fatalf("first stage is not $match: %v", execStages[0])
	}

	if matchStage["is_deleted"] != false {
		t.Errorf("expected is_deleted: false, got %v", matchStage["is_deleted"])
	}
	if matchStage["status"] != "active" {
		t.Errorf("expected status: active, got %v", matchStage["status"])
	}

	// 2. Reference pipeline (parameter metadata preserved)
	var refStages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ReferencePipeline), &refStages); err != nil {
		t.Fatalf("failed to parse reference pipeline: %v", err)
	}
	refMatch := refStages[0]["$match"].(map[string]any)
	paramVal, ok := refMatch["status"].(map[string]any)
	if !ok || paramVal["paramName"] != "status_param" {
		t.Errorf("expected status param metadata in reference pipeline, got %v", refMatch["status"])
	}
}

func TestMongoDataSetCompiler_NestedRelationsUseSelectedFields(t *testing.T) {
	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "orders"},
		JoinCollections: []domain.JoinCollection{
			{FromCollection: "orders", FromCollectionField: "customer_id", ToCollection: "customers", ToCollectionField: "id", NamedAs: "customer", Cardinality: "ONE", OutputFormat: "OBJECT"},
			{FromCollection: "orders", FromCollectionField: "id", ToCollection: "items", ToCollectionField: "order_id", NamedAs: "items", Cardinality: "MANY", OutputFormat: "JSON_ARRAY"},
		},
		SelectedList: []domain.SelectedField{
			{Field: "orders.id", HeaderName: "id"},
			{Field: "customer.name", HeaderName: "customer_name"},
			{Field: "items.sku", HeaderName: "items_sku"},
		},
	}
	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := mongodb.NewMongoDataSetCompiler().Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.ExecutableQuery, `"customer":{"name":"$customer.name"}`) {
		t.Fatalf("expected selected object field, got: %s", compiled.ExecutableQuery)
	}
	if !strings.Contains(compiled.ExecutableQuery, `"items":{"$map"`) || !strings.Contains(compiled.ExecutableQuery, `"sku":"$$item.sku"`) {
		t.Fatalf("expected selected array field mapping, got: %s", compiled.ExecutableQuery)
	}
}

func TestMongoDataSetCompiler_JoinsWithUnwindAndFilter(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "employees",
			Alias: "employees",
		},
		Joins: []planner.ASTJoin{
			{
				Type:      domain.JoinLeft,
				FromTable: "employees",
				FromField: "dept_id",
				ToTable:   "departments",
				ToField:   "id",
				Alias:     "departments",
			},
		},
		WhereFilters: []planner.ASTCondition{
			{
				Table:    "departments",
				Column:   "is_active",
				Operator: "=",
				Value:    true,
			},
		},
		Projections: []planner.ASTProjection{
			{
				SourceTable: "employees",
				SourceField: "name",
				Alias:       "emp_name",
			},
			{
				SourceTable: "departments",
				SourceField: "name",
				Alias:       "dept_name",
			},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse pipeline JSON: %v", err)
	}

	// Must have: $lookup, $unwind, $match (joined), $project
	hasLookup := false
	hasUnwind := false
	hasJoinedMatch := false
	hasProject := false

	for _, s := range stages {
		if lookup, ok := s["$lookup"].(map[string]any); ok {
			hasLookup = true
			if lookup["from"] != "departments" || lookup["as"] != "departments" {
				t.Errorf("unexpected lookup config: %v", lookup)
			}
		}
		if unwind, ok := s["$unwind"].(map[string]any); ok {
			hasUnwind = true
			if unwind["path"] != "$departments" || unwind["preserveNullAndEmptyArrays"] != true {
				t.Errorf("unexpected unwind config: %v", unwind)
			}
		}
		if match, ok := s["$match"].(map[string]any); ok {
			if match["departments.is_active"] == true {
				hasJoinedMatch = true
			}
		}
		if proj, ok := s["$project"].(map[string]any); ok {
			hasProject = true
			if proj["dept_name"] != "$departments.name" {
				t.Errorf("expected dept_name: $departments.name, got %v", proj["dept_name"])
			}
			if proj["emp_name"] != "$name" {
				t.Errorf("expected emp_name: $name, got %v", proj["emp_name"])
			}
		}
	}

	if !hasLookup {
		t.Errorf("missing $lookup stage")
	}
	if !hasUnwind {
		t.Errorf("missing $unwind stage after lookup")
	}
	if !hasJoinedMatch {
		t.Errorf("missing joined collection $match stage")
	}
	if !hasProject {
		t.Errorf("missing $project stage")
	}
}

func TestMongoDataSetCompiler_RowCalculations(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "orders",
			Alias: "orders",
		},
		CustomColumns: []planner.ASTCustomColumn{
			{
				Alias:        "total_amount",
				FunctionName: "MULTIPLY",
				IsAggregate:  false,
				Operands: []planner.ASTOperand{
					{SourceTable: "orders", SourceField: "quantity"},
					{SourceTable: "orders", SourceField: "price"},
				},
			},
			{
				Alias:        "full_code",
				FunctionName: "CONCAT",
				IsAggregate:  false,
				Operands: []planner.ASTOperand{
					{SourceTable: "orders", SourceField: "prefix"},
					{SourceTable: "_LITERAL_", LiteralVal: "-", IsLiteral: true},
					{SourceTable: "orders", SourceField: "code"},
				},
			},
		},
		Projections: []planner.ASTProjection{
			{SourceTable: "orders", SourceField: "id"},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse pipeline JSON: %v", err)
	}

	hasAddFields := false
	for _, s := range stages {
		if addFields, ok := s["$addFields"].(map[string]any); ok {
			hasAddFields = true
			if mult, ok := addFields["total_amount"].(map[string]any); !ok || mult["$multiply"] == nil {
				t.Errorf("expected $multiply in total_amount, got %v", addFields["total_amount"])
			}
			if concat, ok := addFields["full_code"].(map[string]any); !ok || concat["$concat"] == nil {
				t.Errorf("expected $concat in full_code, got %v", addFields["full_code"])
			}
		}
	}

	if !hasAddFields {
		t.Errorf("missing $addFields stage for row-level custom columns")
	}
}

func TestMongoDataSetCompiler_GroupedAggregations(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "sales",
			Alias: "sales",
		},
		GroupBy: []planner.ASTGroupBy{
			{Table: "sales", Field: "region"},
			{Table: "sales", Field: "category"},
		},
		CustomColumns: []planner.ASTCustomColumn{
			{
				Alias:        "total_revenue",
				FunctionName: "SUM",
				IsAggregate:  true,
				Operands: []planner.ASTOperand{
					{SourceTable: "sales", SourceField: "amount"},
				},
			},
			{
				Alias:        "total_orders",
				FunctionName: "COUNT_ALL",
				IsAggregate:  true,
			},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse pipeline JSON: %v", err)
	}

	hasGroup := false
	hasProject := false

	for _, s := range stages {
		if group, ok := s["$group"].(map[string]any); ok {
			hasGroup = true
			idMap, ok := group["_id"].(map[string]any)
			if !ok {
				t.Fatalf("expected composite _id map in $group, got %v", group["_id"])
			}
			if idMap["region"] != "$region" || idMap["category"] != "$category" {
				t.Errorf("unexpected composite _id fields: %v", idMap)
			}
			if sum, ok := group["total_revenue"].(map[string]any); !ok || sum["$sum"] != "$amount" {
				t.Errorf("unexpected total_revenue expression: %v", group["total_revenue"])
			}
			if count, ok := group["total_orders"].(map[string]any); !ok || count["$sum"] != float64(1) {
				t.Errorf("unexpected total_orders count: %v", group["total_orders"])
			}
		}

		if proj, ok := s["$project"].(map[string]any); ok {
			hasProject = true
			if proj["region"] != "$_id.region" {
				t.Errorf("expected region: $_id.region, got %v", proj["region"])
			}
			if proj["category"] != "$_id.category" {
				t.Errorf("expected category: $_id.category, got %v", proj["category"])
			}
			if proj["total_revenue"] != float64(1) {
				t.Errorf("expected total_revenue: 1, got %v", proj["total_revenue"])
			}
		}
	}

	if !hasGroup {
		t.Errorf("missing $group stage")
	}
	if !hasProject {
		t.Errorf("missing post-group $project stage")
	}
}

func TestMongoDataSetCompiler_GlobalAggregations(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "employees",
			Alias: "employees",
		},
		CustomColumns: []planner.ASTCustomColumn{
			{
				Alias:        "avg_salary",
				FunctionName: "AVG",
				IsAggregate:  true,
				Operands: []planner.ASTOperand{
					{SourceTable: "employees", SourceField: "salary"},
				},
			},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse pipeline JSON: %v", err)
	}

	foundGlobalGroup := false
	for _, s := range stages {
		if group, ok := s["$group"].(map[string]any); ok {
			if group["_id"] == nil {
				foundGlobalGroup = true
			}
		}
	}

	if !foundGlobalGroup {
		t.Errorf("expected global $group with _id: null when no group by is specified")
	}
}

func TestMongoDataSetCompiler_CountDistinct(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "employees",
			Alias: "employees",
		},
		GroupBy: []planner.ASTGroupBy{
			{Table: "employees", Field: "department_id"},
		},
		CustomColumns: []planner.ASTCustomColumn{
			{
				Alias:        "unique_roles",
				FunctionName: "COUNT_DISTINCT",
				IsAggregate:  true,
				Operands: []planner.ASTOperand{
					{SourceTable: "employees", SourceField: "role_id"},
				},
			},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse pipeline JSON: %v", err)
	}

	foundSet := false
	foundSize := false

	for _, s := range stages {
		if group, ok := s["$group"].(map[string]any); ok {
			if setExpr, ok := group["unique_roles_set"].(map[string]any); ok && setExpr["$addToSet"] == "$role_id" {
				foundSet = true
			}
		}
		if proj, ok := s["$project"].(map[string]any); ok {
			if sizeExpr, ok := proj["unique_roles"].(map[string]any); ok && sizeExpr["$size"] == "$unique_roles_set" {
				foundSize = true
			}
		}
	}

	if !foundSet {
		t.Errorf("expected $addToSet in group stage for COUNT_DISTINCT")
	}
	if !foundSize {
		t.Errorf("expected $size in post-group project stage for COUNT_DISTINCT")
	}
}

func TestMongoDataSetCompiler_SortAndPagination(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ast := &planner.QueryAST{
		BaseTable: planner.ASTBaseTable{
			Table: "items",
			Alias: "items",
		},
		OrderBy: []planner.ASTOrderBy{
			{Table: "items", Field: "created_at", Desc: true},
		},
		Offset: 10,
		Limit:  25,
		Projections: []planner.ASTProjection{
			{SourceTable: "items", SourceField: "id"},
		},
	}

	ds := &domain.DataSet{SaveMode: domain.SaveModeQuery}
	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	pipelineJSON := compiled.ExecutableQuery
	if !strings.Contains(pipelineJSON, `"$sort":{"created_at":-1}`) {
		t.Errorf("expected $sort stage, got %s", pipelineJSON)
	}
	if !strings.Contains(pipelineJSON, `"$skip":10`) {
		t.Errorf("expected $skip stage, got %s", pipelineJSON)
	}
	if !strings.Contains(pipelineJSON, `"$limit":25`) {
		t.Errorf("expected $limit stage, got %s", pipelineJSON)
	}
}

func TestMongoDataSetCompiler_ArrayRelationship_AnyFrom(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "employees"},
		JoinCollections: []domain.JoinCollection{
			{
				FromCollection:      "employees",
				FromCollectionField: "plant_mine",
				ToCollection:        "plants",
				ToCollectionField:   "id",
				NamedAs:             "plant_details",
				ConvertToString:     true,
				CastMode:            "ANY_FROM",
				Cardinality:         "ONE",
				OutputFormat:        "OBJECT",
			},
		},
		SelectedList: []domain.SelectedField{
			{Field: "employees.id", HeaderName: "id", DataType: "STRING"},
			{Field: "employees.plant_mine", HeaderName: "plant_mine", DataType: "ARRAY"},
			{Field: "plant_details.name", HeaderName: "plant_name", DataType: "STRING"},
		},
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	pipelineJSON := compiled.ExecutableQuery
	// Verify $lookup uses pipeline with $in matching foreign scalar against mapped local array:
	if !strings.Contains(pipelineJSON, `"$in":[{"$toString":"$id"},{"$map"`) {
		t.Fatalf("expected $in condition with $map on localVal, got: %s", pipelineJSON)
	}
	if !strings.Contains(pipelineJSON, `"$toString":"$$el"`) {
		t.Fatalf("expected $toString on mapped array elements, got: %s", pipelineJSON)
	}
	if !strings.Contains(pipelineJSON, `"$lookup"`) {
		t.Fatalf("expected $lookup stage, got: %s", pipelineJSON)
	}
}

func TestMongoDataSetCompiler_ArrayRelationship_AnyTo(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "users"},
		JoinCollections: []domain.JoinCollection{
			{
				FromCollection:      "users",
				FromCollectionField: "role_id",
				ToCollection:        "permissions",
				ToCollectionField:   "allowed_roles",
				NamedAs:             "permissions",
				ConvertToString:     true,
				CastMode:            "ANY_TO",
				Cardinality:         "ONE",
				OutputFormat:        "OBJECT",
			},
		},
		SelectedList: []domain.SelectedField{
			{Field: "users.id", HeaderName: "id", DataType: "STRING"},
			{Field: "users.role_id", HeaderName: "role_id", DataType: "STRING"},
			{Field: "permissions.allowed_roles", HeaderName: "allowed_roles", DataType: "ARRAY"},
		},
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	pipelineJSON := compiled.ExecutableQuery
	// Verify $lookup uses pipeline with $in matching local scalar against mapped foreign array:
	if !strings.Contains(pipelineJSON, `"$in":[{"$toString":"$$localVal"},{"$map"`) {
		t.Fatalf("expected $in condition with scalar localVal and mapped foreign allowed_roles, got: %s", pipelineJSON)
	}
}

func TestMongoDataSetCompiler_ArrayRelationship_AnyBoth(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "employees"},
		JoinCollections: []domain.JoinCollection{
			{
				FromCollection:      "employees",
				FromCollectionField: "tags",
				ToCollection:        "groups",
				ToCollectionField:   "target_tags",
				NamedAs:             "matched_groups",
				ConvertToString:     true,
				CastMode:            "ANY_BOTH",
				Cardinality:         "MANY",
				OutputFormat:        "JSON_ARRAY",
			},
		},
		SelectedList: []domain.SelectedField{
			{Field: "employees.id", HeaderName: "id", DataType: "STRING"},
			{Field: "employees.tags", HeaderName: "tags", DataType: "ARRAY"},
			{Field: "matched_groups.target_tags", HeaderName: "target_tags", DataType: "ARRAY"},
		},
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	pipelineJSON := compiled.ExecutableQuery
	// Verify set intersection is used for both arrays:
	if !strings.Contains(pipelineJSON, `"$setIntersection"`) || !strings.Contains(pipelineJSON, `"$size"`) {
		t.Fatalf("expected $setIntersection and $size condition for ANY_BOTH, got: %s", pipelineJSON)
	}
}

func TestMongoDataSetCompiler_InvertedAnyTo_AutoCorrection(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	// User accidentally selected ANY_TO when source (plant_mine) is an array and target (id) is scalar
	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{Collection: "employees"},
		JoinCollections: []domain.JoinCollection{
			{
				FromCollection:      "employees",
				FromCollectionField: "plant_mine",
				ToCollection:        "plants",
				ToCollectionField:   "id",
				NamedAs:             "plants",
				ConvertToString:     true,
				CastMode:            "ANY_TO", // Inverted!
				Cardinality:         "ONE",
				OutputFormat:        "OBJECT",
			},
		},
		SelectedList: []domain.SelectedField{
			{Field: "employees.id", HeaderName: "id", DataType: "STRING"},
			{Field: "employees.plant_mine", HeaderName: "plant_mine", DataType: "ARRAY"},
			{Field: "plants.id", HeaderName: "plant_id", DataType: "STRING"},
		},
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	pipelineJSON := compiled.ExecutableQuery
	// Verify it was auto-corrected to ANY_FROM ($in: [{"$toString":"$id"}, mapped localVal]):
	if !strings.Contains(pipelineJSON, `"$in":[{"$toString":"$id"},{"$map"`) {
		t.Fatalf("expected auto-correction of inverted ANY_TO to $in: ['$id', localArray], got: %s", pipelineJSON)
	}
}

func TestMongoDataSetCompiler_AvoidsProjectCollisionOnNestedAttributes(t *testing.T) {
	compiler := mongodb.NewMongoDataSetCompiler()

	ds := &domain.DataSet{
		BaseCollection: domain.BaseCollection{
			Collection: "shop",
			Schema:     "public",
		},
		SelectedList: []domain.SelectedField{
			{Field: "shop.contact_number", HeaderName: "contact_number", DataType: "STRING"},
			{Field: "shop.shop_image.storage_name", HeaderName: "shop_image.storage_name", DataType: "STRING"},
			{Field: "shop.shop_image", HeaderName: "shop_image", DataType: "JSON"},
			{Field: "shop.delegates", HeaderName: "delegates", DataType: "ARRAY"},
			{Field: "shop.delegates.time", HeaderName: "delegates.time", DataType: "DATETIME"},
			{Field: "shop.delegates.user_id", HeaderName: "delegates.user_id", DataType: "STRING"},
		},
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiled, err := compiler.Compile(context.Background(), ast, ds)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse executable pipeline: %v", err)
	}

	var projectStage map[string]any
	for _, s := range stages {
		if p, ok := s["$project"].(map[string]any); ok {
			projectStage = p
			break
		}
	}
	if projectStage == nil {
		t.Fatalf("expected $project stage in compiled pipeline, got: %s", compiled.ExecutableQuery)
	}

	// Verify "delegates" is present, but "delegates.time" and "delegates.user_id" are NOT present to prevent collision
	if _, ok := projectStage["delegates"]; !ok {
		t.Errorf("expected 'delegates' in $project")
	}
	if _, ok := projectStage["delegates.time"]; ok {
		t.Errorf("expected 'delegates.time' to be excluded from $project to prevent path collision")
	}
	if _, ok := projectStage["delegates.user_id"]; ok {
		t.Errorf("expected 'delegates.user_id' to be excluded from $project to prevent path collision")
	}

	// Verify "shop_image" (which was listed after shop_image.storage_name) superseded the child
	if _, ok := projectStage["shop_image"]; !ok {
		t.Errorf("expected 'shop_image' in $project")
	}
	if _, ok := projectStage["shop_image.storage_name"]; ok {
		t.Errorf("expected 'shop_image.storage_name' to be excluded from $project because parent 'shop_image' is present")
	}
}

func TestMongoDataSetCompiler_FullUserRawJSONPayload(t *testing.T) {
	rawJSON := `{"name":"asdas","reference_name":"asdasdasd","driver":"mongodb","save_mode":"QUERY","base_collection":{"schema":"public","collection":"shop","filter":{},"_filterClause":"AND","_filterRules":[]},"join_collections":[],"custom_columns":[],"group_by_fields":[],"schematic_table":[],"filter_params":[],"selected_list":[{"field":"shop.contact_number","headerName":"contact_number","dataType":"STRING"},{"field":"shop.contact_name","headerName":"contact_name","dataType":"STRING"},{"field":"shop.timings","headerName":"timings","dataType":"ARRAY"},{"field":"shop.timings.closed","headerName":"timings.closed","dataType":"BOOLEAN"},{"field":"shop.timings.closing","headerName":"timings.closing","dataType":"STRING"},{"field":"shop.timings.day","headerName":"timings.day","dataType":"STRING"},{"field":"shop.timings.opening","headerName":"timings.opening","dataType":"STRING"},{"field":"shop.review_count","headerName":"review_count","dataType":"INT"},{"field":"shop.shop_image.storage_name","headerName":"shop_image.storage_name","dataType":"STRING"},{"field":"shop.shop_image","headerName":"shop_image","dataType":"JSON"},{"field":"shop._id","headerName":"_id","dataType":"STRING"},{"field":"shop.createdAt","headerName":"createdAt","dataType":"DATETIME"},{"field":"shop.delegates","headerName":"delegates","dataType":"ARRAY"},{"field":"shop.delegates.time","headerName":"delegates.time","dataType":"DATETIME"},{"field":"shop.delegates.user_id","headerName":"delegates.user_id","dataType":"STRING"},{"field":"shop.details","headerName":"details","dataType":"STRING"},{"field":"shop.enable_referral_bonus","headerName":"enable_referral_bonus","dataType":"BOOLEAN"},{"field":"shop.isShopDetailsComplete","headerName":"isShopDetailsComplete","dataType":"BOOLEAN"},{"field":"shop.location","headerName":"location","dataType":"ARRAY"},{"field":"shop.main_category","headerName":"main_category","dataType":"STRING"},{"field":"shop.name","headerName":"name","dataType":"STRING"},{"field":"shop.rating","headerName":"rating","dataType":"DECIMAL"},{"field":"shop.referral_points","headerName":"referral_points","dataType":"DECIMAL"},{"field":"shop.return_points_percent","headerName":"return_points_percent","dataType":"DECIMAL"},{"field":"shop.shop_image._id","headerName":"shop_image._id","dataType":"STRING"},{"field":"shop.shop_image.file_name","headerName":"shop_image.file_name","dataType":"STRING"},{"field":"shop.shop_image.ref_id","headerName":"shop_image.ref_id","dataType":"STRING"},{"field":"shop.shop_image.size","headerName":"shop_image.size","dataType":"DECIMAL"},{"field":"shop.shop_image.uploaded_by","headerName":"shop_image.uploaded_by","dataType":"STRING"},{"field":"shop.shop_image.uploaded_on","headerName":"shop_image.uploaded_on","dataType":"STRING"},{"field":"shop.shop_location","headerName":"shop_location","dataType":"JSON"},{"field":"shop.shop_location.lat","headerName":"shop_location.lat","dataType":"DECIMAL"},{"field":"shop.shop_location.lng","headerName":"shop_location.lng","dataType":"DECIMAL"},{"field":"shop.slabs","headerName":"slabs","dataType":"ARRAY"},{"field":"shop.slabs.max","headerName":"slabs.max","dataType":"DECIMAL"},{"field":"shop.slabs.min","headerName":"slabs.min","dataType":"DECIMAL"},{"field":"shop.slabs.money","headerName":"slabs.money","dataType":"DECIMAL"},{"field":"shop.slabs.points","headerName":"slabs.points","dataType":"DECIMAL"},{"field":"shop.street_view_image","headerName":"street_view_image","dataType":"JSON"},{"field":"shop.street_view_image._id","headerName":"street_view_image._id","dataType":"STRING"},{"field":"shop.street_view_image.file_name","headerName":"street_view_image.file_name","dataType":"STRING"},{"field":"shop.street_view_image.ref_id","headerName":"street_view_image.ref_id","dataType":"STRING"},{"field":"shop.street_view_image.size","headerName":"street_view_image.size","dataType":"DECIMAL"},{"field":"shop.street_view_image.storage_name","headerName":"street_view_image.storage_name","dataType":"STRING"},{"field":"shop.street_view_image.uploaded_by","headerName":"street_view_image.uploaded_by","dataType":"STRING"},{"field":"shop.street_view_image.uploaded_on","headerName":"street_view_image.uploaded_on","dataType":"STRING"},{"field":"shop.sub_category","headerName":"sub_category","dataType":"STRING"},{"field":"shop.total_views","headerName":"total_views","dataType":"INT"},{"field":"shop.update_by","headerName":"update_by","dataType":"STRING"},{"field":"shop.update_on","headerName":"update_on","dataType":"DATETIME"},{"field":"shop.updatedAt","headerName":"updatedAt","dataType":"DATETIME"}],"_filterRules":[],"_filterClause":"AND","filter":{}}`

	var ds domain.DataSet
	if err := json.Unmarshal([]byte(rawJSON), &ds); err != nil {
		t.Fatalf("failed to unmarshal dataset json: %v", err)
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), &ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiler := mongodb.NewMongoDataSetCompiler()
	compiled, err := compiler.Compile(context.Background(), ast, &ds)
	if err != nil {
		t.Fatalf("failed to compile pipeline: %v", err)
	}

	var stages []map[string]any
	if err := json.Unmarshal([]byte(compiled.ExecutableQuery), &stages); err != nil {
		t.Fatalf("failed to parse executable pipeline: %v", err)
	}

	var projectStage map[string]any
	for _, s := range stages {
		if p, ok := s["$project"].(map[string]any); ok {
			projectStage = p
			break
		}
	}
	if projectStage == nil {
		t.Fatalf("missing $project stage in compiled pipeline")
	}

	// Verify no path collisions exist in projectStage:
	// For any pair of keys (k1, k2), k1 must not have prefix k2 + "."
	for k1 := range projectStage {
		for k2 := range projectStage {
			if k1 != k2 && strings.HasPrefix(k1, k2+".") {
				t.Errorf("Path collision detected: key '%s' is a child of key '%s' in $project", k1, k2)
			}
		}
	}
}

func TestMongoDataSetCompiler_UserJoinedClaimsWithNestedBillImage(t *testing.T) {
	rawJSON := `{
		"name":"asdas",
		"reference_name":"asdasdasd",
		"driver":"mongodb",
		"save_mode":"QUERY",
		"base_collection":{"schema":"public","collection":"shop","filter":{},"_filterClause":"AND","_filterRules":[]},
		"join_collections":[
			{
				"schema":"public",
				"fromCollection":"shop",
				"fromCollectionField":"_id",
				"toCollection":"claims",
				"toCollectionField":"shop_id",
				"namedAs":"claims_alias",
				"joinType":"LEFT",
				"convert_To_String":false,
				"filter":{},
				"_filterClause":"AND",
				"_filterRules":[],
				"cardinality":"MANY",
				"outputFormat":"JSON_ARRAY",
				"fromFieldDataType":"string",
				"toFieldDataType":"string",
				"cast_mode":"BOTH"
			}
		],
		"custom_columns":[],
		"group_by_fields":[],
		"schematic_table":[],
		"filter_params":[],
		"selected_list":[
			{"field":"shop.contact_name","headerName":"contact_name","dataType":"STRING"},
			{"field":"shop.contact_number","headerName":"contact_number","dataType":"STRING"},
			{"field":"shop._id","headerName":"_id","dataType":"STRING"},
			{"field":"shop.shop_image.storage_name","headerName":"shop_image.storage_name","dataType":"STRING"},
			{"field":"shop.street_view_image.file_name","headerName":"street_view_image.file_name","dataType":"STRING"},
			{"field":"shop.street_view_image.ref_id","headerName":"street_view_image.ref_id","dataType":"STRING"},
			{"field":"shop.street_view_image.size","headerName":"street_view_image.size","dataType":"DECIMAL"},
			{"field":"shop.shop_location","headerName":"shop_location","dataType":"JSON"},
			{"field":"claims_alias.amount","headerName":"amount","dataType":"DECIMAL"},
			{"field":"claims_alias.bill_image","headerName":"bill_image","dataType":"JSON"},
			{"field":"claims_alias.bill_image._id","headerName":"bill_image._id","dataType":"STRING"},
			{"field":"claims_alias.bill_image.file_name","headerName":"bill_image.file_name","dataType":"STRING"},
			{"field":"claims_alias.bill_number","headerName":"bill_number","dataType":"STRING"},
			{"field":"claims_alias.bill_image.uploaded_on","headerName":"bill_image.uploaded_on","dataType":"STRING"},
			{"field":"claims_alias.reason","headerName":"reason","dataType":"STRING"},
			{"field":"claims_alias.shop_id","headerName":"shop_id","dataType":"STRING"},
			{"field":"claims_alias.points","headerName":"points","dataType":"DECIMAL"},
			{"field":"claims_alias.status","headerName":"status","dataType":"STRING"},
			{"field":"claims_alias.user_id","headerName":"user_id","dataType":"STRING"},
			{"field":"claims_alias.created_on","headerName":"created_on","dataType":"DATETIME"},
			{"field":"claims_alias.bill_image.size","headerName":"bill_image.size","dataType":"DECIMAL"},
			{"field":"claims_alias.bill_image.storage_name","headerName":"bill_image.storage_name","dataType":"STRING"},
			{"field":"claims_alias.bill_image.uploaded_by","headerName":"bill_image.uploaded_by","dataType":"STRING"}
		],
		"_filterRules":[],
		"_filterClause":"AND",
		"filter":{}
	}`

	var ds domain.DataSet
	if err := json.Unmarshal([]byte(rawJSON), &ds); err != nil {
		t.Fatalf("failed to unmarshal dataset json: %v", err)
	}

	ast, err := planner.NewPlanner(nil).BuildAST(context.Background(), &ds)
	if err != nil {
		t.Fatalf("failed to build AST: %v", err)
	}

	compiler := mongodb.NewMongoDataSetCompiler()
	compiled, err := compiler.Compile(context.Background(), ast, &ds)
	if err != nil {
		t.Fatalf("failed to compile pipeline: %v", err)
	}

	pipelineJSON := compiled.ExecutableQuery

	// Assert that $map's in object has no keys with '.' like "bill_image._id"
	if strings.Contains(pipelineJSON, `"bill_image._id"`) {
		t.Fatalf("Found invalid key 'bill_image._id' in pipeline: %s", pipelineJSON)
	}
	if strings.Contains(pipelineJSON, `"bill_image.file_name"`) {
		t.Fatalf("Found invalid key 'bill_image.file_name' in pipeline: %s", pipelineJSON)
	}
	// Assert bill_image is cleanly projected as "$$item.bill_image"
	if !strings.Contains(pipelineJSON, `"bill_image":"$$item.bill_image"`) {
		t.Fatalf("Expected '\"bill_image\":\"$$item.bill_image\"' in pipeline, got: %s", pipelineJSON)
	}
}



