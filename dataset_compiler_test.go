package mongodb_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SanjayDrop5528/models-go-mongodb"
	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
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
