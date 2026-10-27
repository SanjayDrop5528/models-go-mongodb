// Package mongodb provides dataset aggregation pipeline compilation for MongoDB.
//
// Usage:
// This file transforms an engine QueryAST and DataSet definition into a MongoDB aggregation
// pipeline JSON string. It supports:
// 1. Initial and post-lookup $match stages for WHERE filters.
// 2. $lookup and $unwind stages for relational joins across collections.
// 3. $addFields / $project stages for custom calculated columns and formula expressions.
// 4. $group stages with MongoDB accumulators ($sum, $avg, $min, $max, $push, $addToSet).
// 5. Parameterized pipeline outputs preserving {"paramName": "...", "paramDataType": "..."} tokens.
package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SanjayDrop5528/models-go-engine/adapter"
	"github.com/SanjayDrop5528/models-go-engine/dataset/compiler"
	"github.com/SanjayDrop5528/models-go-engine/dataset/domain"
	"github.com/SanjayDrop5528/models-go-engine/dataset/planner"
)

// MongoDataSetCompiler compiles QueryAST into MongoDB aggregation pipelines.
type MongoDataSetCompiler struct{}

// NewMongoDataSetCompiler creates a new MongoDB dataset compiler instance.
//
// Purpose:
// Instantiates a MongoDataSetCompiler capable of translating an engine QueryAST into MongoDB aggregation stages.
//
// Where it is used:
// Used in MongoAdapter.CompileDataSet, MongoAdapter.DataSetCompiler, and directly in unit tests and service registrations.
//
// When can it be used:
// Can be used during application initialization or adapter registration to provide MongoDB aggregation pipeline compilation.
//
// Parameters:
//   - None.
//
// Returns:
//   - result1 (*MongoDataSetCompiler): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by NewMongoDataSetCompiler and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - This function does not return an error.
//
// Note:
//   - Callers should preserve the documented ownership and lifecycle of supplied values.
func NewMongoDataSetCompiler() *MongoDataSetCompiler {
	return &MongoDataSetCompiler{}
}

// Compile compiles the QueryAST into MongoDB JSON aggregation pipeline.
//
// Purpose:
// Compiles a database-agnostic QueryAST into an executable JSON aggregation pipeline string and a reference pipeline.
//
// Where it is used:
// Invoked by DataSetService.Preview, DataSetService.Save, and MongoAdapter.CompileDataSet.
//
// When can it be used:
// Can be used whenever a DataSet definition has been planned and needs to be executed as an aggregation pipeline in MongoDB.
//
// Parameters:
//   - ctx (context.Context): Input value used by the operation.
//   - ast (*planner.QueryAST): Input value used by the operation.
//   - ds (*domain.DataSet): Input value used by the operation.
//
// Returns:
//   - result1 (*compiler.CompiledPipeline): Returned value.
//   - result2 (error): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by Compile and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - Returns validation, configuration, context, or underlying dependency errors.
//
// Note:
//   - The context controls cancellation and request lifetime.
func (c *MongoDataSetCompiler) Compile(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	if ast == nil {
		return nil, domain.NewError(domain.ErrPipelineCompilationFailed, "cannot compile nil AST")
	}

	enrichASTJoinTypes(ast, ds)

	execPipeline := c.buildPipelineJSON(ast, false)
	refPipeline := c.buildPipelineJSON(ast, true)

	return &compiler.CompiledPipeline{
		ExecutableQuery:   execPipeline,
		ReferencePipeline: refPipeline,
		Parameters:        ast.Parameters,
		DDLStatement:      "", // No stored procedures in MongoDB
		SaveMode:          ds.SaveMode,
		Driver:            "mongodb",
	}, nil
}

// buildPipelineJSON constructs the MongoDB aggregation pipeline as a JSON array string.
//
// Purpose:
// Assembles all aggregation stages ($match, $lookup, $unwind, $addFields, $group, $project) from the QueryAST into valid JSON.
//
// Where it is used:
// Called internally by Compile to generate executable and reference pipeline JSON strings.
//
// When can it be used:
// Can be used during compilation whenever a QueryAST must be serialized into a MongoDB aggregation pipeline.
func (c *MongoDataSetCompiler) buildPipelineJSON(ast *planner.QueryAST, parameterized bool) string {
	var stages []map[string]any

	// 1. Initial $match stage for base collection filter
	matchStage := make(map[string]any)
	if len(ast.BaseTable.Filter) > 0 {
		for k, v := range ast.BaseTable.Filter {
			matchStage[k] = v
		}
	}

	for _, cond := range ast.WhereFilters {
		if cond.Table == "" || cond.Table == ast.BaseTable.Table || cond.Table == ast.BaseTable.Alias {
			c.applyFilterCondition(matchStage, cond.Column, cond, parameterized, ast.Parameters)
		}
	}

	if len(matchStage) > 0 {
		stages = append(stages, map[string]any{"$match": matchStage})
	}

	// 2. $lookup and immediate $unwind stages for joins
	for _, j := range ast.Joins {
		lookupStage := c.buildMongoJoinLookup(j)
		stages = append(stages, lookupStage)

		// Keep MANY/JSON_ARRAY lookups as arrays; singular joins are unwound to objects.
		if strings.EqualFold(j.Cardinality, "MANY") || strings.EqualFold(j.OutputFormat, "JSON_ARRAY") {
			continue
		}
		preserveNull := true
		if j.Type == domain.JoinInner {
			preserveNull = false
		}
		unwindStage := map[string]any{
			"$unwind": map[string]any{
				"path":                       fmt.Sprintf("$%s", j.Alias),
				"preserveNullAndEmptyArrays": preserveNull,
			},
		}
		stages = append(stages, unwindStage)
	}

	// 3. Post-join $match stage for filters applied to joined tables
	joinedMatchStage := make(map[string]any)
	for _, cond := range ast.WhereFilters {
		if cond.Table != "" && cond.Table != ast.BaseTable.Table && cond.Table != ast.BaseTable.Alias {
			path := c.resolveFieldPath(cond.Table, cond.Column, ast)
			c.applyFilterCondition(joinedMatchStage, path, cond, parameterized, ast.Parameters)
		}
	}
	if len(joinedMatchStage) > 0 {
		stages = append(stages, map[string]any{"$match": joinedMatchStage})
	}

	// 4. Non-aggregate custom calculations via $addFields
	addFieldsStage := make(map[string]any)
	for _, cc := range ast.CustomColumns {
		if !cc.IsAggregate {
			expr := c.buildMongoExpression(cc, ast)
			if expr != nil {
				addFieldsStage[cc.Alias] = expr
			}
		}
	}
	if len(addFieldsStage) > 0 {
		stages = append(stages, map[string]any{"$addFields": addFieldsStage})
	}

	// 5. $group stage if GroupBy or Aggregate columns are present
	hasAggregates := false
	var distinctAggregates []string
	for _, cc := range ast.CustomColumns {
		if cc.IsAggregate {
			hasAggregates = true
			fn := strings.ToUpper(strings.TrimSpace(cc.FunctionName))
			if cc.Function != nil {
				fn = strings.ToUpper(strings.TrimSpace(cc.Function.Name))
			}
			if fn == "COUNT_DISTINCT" {
				distinctAggregates = append(distinctAggregates, cc.Alias)
			}
		}
	}

	isGrouped := len(ast.GroupBy) > 0 || hasAggregates
	if isGrouped {
		groupStage := make(map[string]any)

		if len(ast.GroupBy) == 0 {
			groupStage["_id"] = nil
		} else if len(ast.GroupBy) == 1 {
			groupStage["_id"] = c.resolveFieldRef(ast.GroupBy[0].Table, ast.GroupBy[0].Field, ast)
		} else {
			idMap := make(map[string]any)
			for _, g := range ast.GroupBy {
				idMap[g.Field] = c.resolveFieldRef(g.Table, g.Field, ast)
			}
			groupStage["_id"] = idMap
		}

		for _, cc := range ast.CustomColumns {
			if cc.IsAggregate {
				fn := strings.ToUpper(strings.TrimSpace(cc.FunctionName))
				if cc.Function != nil {
					fn = strings.ToUpper(strings.TrimSpace(cc.Function.Name))
				}

				var opField any = 1
				if len(cc.Operands) > 0 {
					opField = c.formatMongoOperand(cc.Operands[0], ast)
				}

				switch fn {
				case "SUM":
					groupStage[cc.Alias] = map[string]any{"$sum": opField}
				case "AVG":
					groupStage[cc.Alias] = map[string]any{"$avg": opField}
				case "MIN":
					groupStage[cc.Alias] = map[string]any{"$min": opField}
				case "MAX":
					groupStage[cc.Alias] = map[string]any{"$max": opField}
				case "COUNT_ALL", "COUNT(*)":
					groupStage[cc.Alias] = map[string]any{"$sum": 1}
				case "COUNT":
					if opField == 1 || opField == "$*" || opField == "*" {
						groupStage[cc.Alias] = map[string]any{"$sum": 1}
					} else {
						groupStage[cc.Alias] = map[string]any{
							"$sum": map[string]any{
								"$cond": []any{
									map[string]any{"$ifNull": []any{opField, false}},
									1,
									0,
								},
							},
						}
					}
				case "COUNT_DISTINCT":
					groupStage[cc.Alias+"_set"] = map[string]any{"$addToSet": opField}
				case "COUNT_IF":
					groupStage[cc.Alias] = map[string]any{
						"$sum": map[string]any{
							"$cond": []any{opField, 1, 0},
						},
					}
				case "SUM_IF":
					var sumVal any = 1
					if len(cc.Operands) > 1 {
						sumVal = c.formatMongoOperand(cc.Operands[1], ast)
					}
					groupStage[cc.Alias] = map[string]any{
						"$sum": map[string]any{
							"$cond": []any{opField, sumVal, 0},
						},
					}
				default:
					groupStage[cc.Alias] = map[string]any{"$sum": 1}
				}
			}
		}

		stages = append(stages, map[string]any{"$group": groupStage})

		// Post-group projection: unpack grouped keys and distinct count sizes
		postGroupProject := make(map[string]any)
		if len(ast.GroupBy) == 1 {
			setProjectField(postGroupProject, ast.GroupBy[0].Field, "$_id")
		} else if len(ast.GroupBy) > 1 {
			for _, g := range ast.GroupBy {
				setProjectField(postGroupProject, g.Field, fmt.Sprintf("$_id.%s", g.Field))
			}
		}

		for _, cc := range ast.CustomColumns {
			if cc.IsAggregate {
				isDistinct := false
				for _, d := range distinctAggregates {
					if d == cc.Alias {
						isDistinct = true
						break
					}
				}
				if isDistinct {
					setProjectField(postGroupProject, cc.Alias, map[string]any{"$size": fmt.Sprintf("$%s_set", cc.Alias)})
				} else {
					setProjectField(postGroupProject, cc.Alias, 1)
				}
			}
		}

		postGroupProject["_id"] = 0
		stages = append(stages, map[string]any{"$project": postGroupProject})
	} else {
		// 6. Final $project stage when not grouped
		projectStage := make(map[string]any)
		hasExplicitID := false

		for _, p := range ast.Projections {
			if mongoNestedJoin(ast, p.SourceTable) != nil {
				continue
			}
			alias := p.Alias
			if alias == "" {
				alias = p.SourceField
			}
			if alias == "_id" || strings.HasPrefix(alias, "_id.") {
				hasExplicitID = true
			}

			path := c.resolveFieldRef(p.SourceTable, p.SourceField, ast)
			if alias == strings.TrimPrefix(path, "$") {
				setProjectField(projectStage, alias, 1)
			} else {
				setProjectField(projectStage, alias, path)
			}
		}

		for i := range ast.Joins {
			join := &ast.Joins[i]
			if strings.EqualFold(join.OutputFormat, "NONE") || strings.EqualFold(join.Cardinality, "NONE") {
				continue
			}
			if !strings.EqualFold(join.OutputFormat, "OBJECT") && !strings.EqualFold(join.OutputFormat, "JSON_ARRAY") && !strings.EqualFold(join.Cardinality, "MANY") {
				continue
			}
			fields := mongoRelationProjections(ast, *join)
			if strings.EqualFold(join.Cardinality, "MANY") || strings.EqualFold(join.OutputFormat, "JSON_ARRAY") {
				item := buildMongoRelationItem(*join, fields, "$$item.")
				setProjectField(projectStage, join.Alias, map[string]any{
					"$map": map[string]any{"input": "$" + join.Alias, "as": "item", "in": item},
				})
			} else {
				object := buildMongoRelationItem(*join, fields, "$"+join.Alias+".")
				setProjectField(projectStage, join.Alias, object)
			}
		}

		for _, cc := range ast.CustomColumns {
			if cc.Alias == "_id" || strings.HasPrefix(cc.Alias, "_id.") {
				hasExplicitID = true
			}
			setProjectField(projectStage, cc.Alias, 1)
		}

		if len(projectStage) > 0 {
			if !hasExplicitID {
				projectStage["_id"] = 0
			}
			stages = append(stages, map[string]any{"$project": projectStage})
		}
	}

	// 7. $sort stage
	if len(ast.OrderBy) > 0 {
		sortStage := make(map[string]any)
		for _, o := range ast.OrderBy {
			fieldPath := c.resolveFieldPath(o.Table, o.Field, ast)
			dir := 1
			if o.Desc {
				dir = -1
			}
			sortStage[fieldPath] = dir
		}
		stages = append(stages, map[string]any{"$sort": sortStage})
	}

	// 8. $skip stage
	if ast.Offset > 0 {
		stages = append(stages, map[string]any{"$skip": ast.Offset})
	}

	// 9. $limit stage
	if ast.Limit > 0 {
		stages = append(stages, map[string]any{"$limit": ast.Limit})
	}

	bytes, _ := json.Marshal(stages)
	return string(bytes)
}

// mongoNestedJoin returns the nested-output join for an alias, if one exists.
func mongoNestedJoin(ast *planner.QueryAST, sourceTable string) *planner.ASTJoin {
	for i := range ast.Joins {
		join := &ast.Joins[i]
		if strings.EqualFold(join.OutputFormat, "NONE") || strings.EqualFold(join.Cardinality, "NONE") {
			continue
		}
		if strings.EqualFold(join.Alias, sourceTable) && (strings.EqualFold(join.OutputFormat, "OBJECT") || strings.EqualFold(join.OutputFormat, "JSON_ARRAY") || strings.EqualFold(join.Cardinality, "MANY")) {
			return join
		}
	}
	return nil
}

// mongoRelationProjections returns fields selected from a specific joined relation.
func mongoRelationProjections(ast *planner.QueryAST, join planner.ASTJoin) []planner.ASTProjection {
	var fields []planner.ASTProjection
	for _, projection := range ast.Projections {
		if strings.EqualFold(projection.SourceTable, join.Alias) {
			fields = append(fields, projection)
		}
	}
	return fields
}

// mongoProjectionKey returns the nested document key for a relation field.
func mongoProjectionKey(join planner.ASTJoin, projection planner.ASTProjection) string {
	if projection.Alias == "" || projection.Alias == projection.SourceField || strings.EqualFold(projection.Alias, join.Alias+"_"+projection.SourceField) {
		return projection.SourceField
	}
	return projection.Alias
}

// buildMongoRelationItem builds a nested projection document for a relation, avoiding field names with '.'
// and resolving parent-child redundancies (e.g., if "bill_image" is selected, sub-paths like "bill_image._id" are skipped).
func buildMongoRelationItem(join planner.ASTJoin, fields []planner.ASTProjection, varPrefix string) map[string]any {
	result := make(map[string]any)

	// Set of parent fields that are explicitly projected as full objects
	parentSet := make(map[string]bool)
	for _, f := range fields {
		if !strings.Contains(f.SourceField, ".") {
			parentSet[f.SourceField] = true
		}
	}

	for _, field := range fields {
		src := field.SourceField
		if !strings.Contains(src, ".") {
			key := mongoProjectionKey(join, field)
			result[key] = varPrefix + src
			continue
		}

		parts := strings.Split(src, ".")
		rootParent := parts[0]

		// If root parent is already projected as a full document, skip subfields
		if parentSet[rootParent] {
			continue
		}

		// Navigate or create nested subdocument
		curr := result
		for i := 0; i < len(parts)-1; i++ {
			part := parts[i]
			if existing, ok := curr[part]; ok {
				if m, ok := existing.(map[string]any); ok {
					curr = m
				} else {
					newM := make(map[string]any)
					curr[part] = newM
					curr = newM
				}
			} else {
				newM := make(map[string]any)
				curr[part] = newM
				curr = newM
			}
		}
		leafKey := parts[len(parts)-1]
		if field.Alias != "" && !strings.Contains(field.Alias, ".") && field.Alias != src {
			leafKey = field.Alias
		}
		curr[leafKey] = varPrefix + src
	}

	return result
}

// resolveFieldPath resolves a table-qualified column into its MongoDB document field path.
//
// Purpose:
// Translates a table and column reference into either a root field name or a nested join path (e.g., "author.name").
//
// Where it is used:
// In resolveFieldRef and $sort, $project, and $group stage assembly.
//
// When can it be used:
// Can be used whenever mapping a relational table.column pair to a document path in MongoDB.
func (c *MongoDataSetCompiler) resolveFieldPath(table, field string, ast *planner.QueryAST) string {
	if table == "" || table == ast.BaseTable.Table || table == ast.BaseTable.Alias {
		return field
	}
	for _, j := range ast.Joins {
		if j.Alias != "" && (table == j.Alias || table == j.ToTable) {
			return fmt.Sprintf("%s.%s", j.Alias, field)
		}
	}
	return fmt.Sprintf("%s.%s", table, field)
}

// resolveFieldRef resolves a table and field into a MongoDB expression field reference (prefixed with "$").
//
// Purpose:
// Prepends "$" to the resolved document field path so it can be evaluated as an expression in MongoDB stages.
//
// Where it is used:
// In formatMongoOperand and aggregation accumulators ($sum, $avg, $min, $max).
//
// When can it be used:
// Can be used whenever an aggregation expression needs a dynamic field reference.
func (c *MongoDataSetCompiler) resolveFieldRef(table, field string, ast *planner.QueryAST) string {
	return "$" + c.resolveFieldPath(table, field, ast)
}

// formatMongoOperand converts an ASTOperand into a MongoDB expression value or field reference.
//
// Purpose:
// Formats literals as raw Go values and column references as "$" field references.
//
// Where it is used:
// In buildMongoExpression when evaluating formula arguments and custom columns.
//
// When can it be used:
// Can be used whenever converting an operand into a MongoDB aggregation expression value.
func (c *MongoDataSetCompiler) formatMongoOperand(op planner.ASTOperand, ast *planner.QueryAST) any {
	if op.IsLiteral || op.SourceTable == "_LITERAL_" {
		return op.LiteralVal
	}
	if op.SourceTable == "" || op.SourceTable == "CALC" {
		return "$" + op.SourceField
	}
	return c.resolveFieldRef(op.SourceTable, op.SourceField, ast)
}

// applyFilterCondition applies a filter condition to a MongoDB $match stage map.
//
// Purpose:
// Converts abstract filter operators (=, !=, >, >=, <, <=, IN, NOT IN, LIKE, IS NULL) into MongoDB query operators.
// Also handles runtime parameter substitution or preservation of parameterized placeholders.
//
// Where it is used:
// In buildPipelineJSON for root collection WHERE filters and post-lookup join filters.
//
// When can it be used:
// Can be used whenever filtering MongoDB documents using relational condition ASTs.
func (c *MongoDataSetCompiler) applyFilterCondition(matchStage map[string]any, path string, cond planner.ASTCondition, parameterized bool, params []domain.FilterParam) {
	var val any
	if cond.IsParamRef {
		if parameterized {
			val = map[string]any{
				"paramName":     cond.ParamName,
				"paramDataType": cond.ParamDataType,
			}
		} else {
			for _, p := range params {
				if strings.EqualFold(p.ParamName, cond.ParamName) {
					val = p.Paramvalue
					if val == nil || val == "" {
						val = p.DefaultValue
					}
					if val != nil {
						break
					}
				}
			}
			if val == nil && cond.Value != nil {
				val = cond.Value
			}
		}
	} else {
		val = cond.Value
	}

	op := strings.ToUpper(strings.TrimSpace(cond.Operator))
	if op == "" {
		op = "="
	}

	switch op {
	case "=", "EQ":
		matchStage[path] = val
	case "!=", "<>", "NEQ":
		matchStage[path] = map[string]any{"$ne": val}
	case ">", "GT":
		matchStage[path] = map[string]any{"$gt": val}
	case ">=", "GTE":
		matchStage[path] = map[string]any{"$gte": val}
	case "<", "LT":
		matchStage[path] = map[string]any{"$lt": val}
	case "<=", "LTE":
		matchStage[path] = map[string]any{"$lte": val}
	case "IN":
		matchStage[path] = map[string]any{"$in": val}
	case "NOT IN", "NIN":
		matchStage[path] = map[string]any{"$nin": val}
	case "LIKE", "ILIKE":
		if s, ok := val.(string); ok {
			regexPattern := strings.ReplaceAll(s, "%", ".*")
			matchStage[path] = map[string]any{"$regex": regexPattern, "$options": "i"}
		} else {
			matchStage[path] = val
		}
	case "IS NULL":
		matchStage[path] = nil
	case "IS NOT NULL":
		matchStage[path] = map[string]any{"$ne": nil, "$exists": true}
	default:
		matchStage[path] = val
	}
}

// buildMongoExpression compiles a custom column formula or function call into a MongoDB expression tree.
//
// Purpose:
// Maps abstract functions (ADD, SUBTRACT, MULTIPLY, DIVIDE, CONCAT, UPPER, LOWER, DATEDIFF, ROUND, etc.)
// into MongoDB expression operators ($add, $subtract, $multiply, $divide, $concat, $toUpper, etc.).
//
// Where it is used:
// In buildPipelineJSON during $addFields and $project stage creation.
//
// When can it be used:
// Can be used whenever calculating derived fields or mathematical expressions in MongoDB.
func (c *MongoDataSetCompiler) buildMongoExpression(cc planner.ASTCustomColumn, ast *planner.QueryAST) any {
	fnName := ""
	if cc.Function != nil {
		fnName = cc.Function.Name
	} else if cc.FunctionName != "" {
		fnName = cc.FunctionName
	}
	fn := strings.ToUpper(strings.TrimSpace(fnName))

	operands := cc.Operands
	var firstArg any
	if len(operands) > 0 {
		firstArg = c.formatMongoOperand(operands[0], ast)
	}

	switch fn {
	case "ADD":
		var args []any
		for _, op := range operands {
			args = append(args, c.formatMongoOperand(op, ast))
		}
		if len(args) == 0 {
			return nil
		}
		return map[string]any{"$add": args}
	case "SUBTRACT":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$subtract": []any{c.formatMongoOperand(operands[0], ast), c.formatMongoOperand(operands[1], ast)}}
	case "MULTIPLY":
		var args []any
		for _, op := range operands {
			args = append(args, c.formatMongoOperand(op, ast))
		}
		if len(args) == 0 {
			return nil
		}
		return map[string]any{"$multiply": args}
	case "DIVIDE":
		if len(operands) < 2 {
			return nil
		}
		op1 := c.formatMongoOperand(operands[0], ast)
		op2 := c.formatMongoOperand(operands[1], ast)
		return map[string]any{
			"$cond": []any{
				map[string]any{"$eq": []any{op2, 0}},
				nil,
				map[string]any{"$divide": []any{op1, op2}},
			},
		}
	case "CONCAT":
		var args []any
		for _, op := range operands {
			val := c.formatMongoOperand(op, ast)
			args = append(args, map[string]any{"$toString": val})
		}
		if len(args) == 0 {
			return nil
		}
		return map[string]any{"$concat": args}
	case "CONCAT_WS":
		if len(operands) < 2 {
			return nil
		}
		sep := c.formatMongoOperand(operands[0], ast)
		var args []any
		for i, op := range operands[1:] {
			if i > 0 {
				args = append(args, sep)
			}
			args = append(args, map[string]any{"$toString": c.formatMongoOperand(op, ast)})
		}
		return map[string]any{"$concat": args}
	case "UPPER":
		return map[string]any{"$toUpper": firstArg}
	case "LOWER":
		return map[string]any{"$toLower": firstArg}
	case "TRIM":
		return map[string]any{"$trim": map[string]any{"input": map[string]any{"$toString": firstArg}}}
	case "LENGTH":
		return map[string]any{"$strLenCP": map[string]any{"$toString": firstArg}}
	case "YEAR":
		return map[string]any{"$year": map[string]any{"$toDate": firstArg}}
	case "MONTH":
		return map[string]any{"$month": map[string]any{"$toDate": firstArg}}
	case "DAY":
		return map[string]any{"$dayOfMonth": map[string]any{"$toDate": firstArg}}
	case "DATE_DIFF", "DATEDIFF":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{
			"$dateDiff": map[string]any{
				"startDate": map[string]any{"$toDate": c.formatMongoOperand(operands[0], ast)},
				"endDate":   map[string]any{"$toDate": c.formatMongoOperand(operands[1], ast)},
				"unit":      "day",
			},
		}
	case "ABS":
		return map[string]any{"$abs": firstArg}
	case "SQRT":
		return map[string]any{"$sqrt": firstArg}
	case "CEIL", "CEILING":
		return map[string]any{"$ceil": firstArg}
	case "FLOOR":
		return map[string]any{"$floor": firstArg}
	case "MODULO", "MOD":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$mod": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "POWER", "POW":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$pow": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "ROUND":
		if len(operands) >= 2 {
			return map[string]any{"$round": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
		}
		return map[string]any{"$round": []any{firstArg, 0}}
	case "SUBSTRING":
		if len(operands) < 3 {
			return nil
		}
		return map[string]any{"$substrCP": []any{firstArg, c.formatMongoOperand(operands[1], ast), c.formatMongoOperand(operands[2], ast)}}
	case "REPLACE":
		if len(operands) < 3 {
			return nil
		}
		return map[string]any{"$replaceAll": map[string]any{
			"input":       firstArg,
			"find":        c.formatMongoOperand(operands[1], ast),
			"replacement": c.formatMongoOperand(operands[2], ast),
		}}
	case "NOW", "CURRENT_DATE":
		return "$$NOW"
	case "DATE_ADD":
		if len(operands) < 2 {
			return nil
		}
		unit := "day"
		if len(operands) >= 3 {
			unit = fmt.Sprintf("%v", c.formatMongoOperand(operands[2], ast))
		}
		return map[string]any{
			"$dateAdd": map[string]any{
				"startDate": map[string]any{"$toDate": firstArg},
				"unit":      strings.ToLower(unit),
				"amount":    c.formatMongoOperand(operands[1], ast),
			},
		}
	case "EQUAL":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$eq": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "NOT_EQUAL":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$ne": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "GREATER_THAN":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$gt": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "LESS_THAN":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{"$lt": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "COALESCE":
		if len(operands) < 2 {
			return firstArg
		}
		return map[string]any{"$ifNull": []any{firstArg, c.formatMongoOperand(operands[1], ast)}}
	case "CASE", "IF":
		if len(operands) >= 3 {
			return map[string]any{"$cond": []any{firstArg, c.formatMongoOperand(operands[1], ast), c.formatMongoOperand(operands[2], ast)}}
		}
		return firstArg
	case "TO_STRING":
		return map[string]any{"$toString": firstArg}
	case "TO_INTEGER":
		return map[string]any{"$toInt": firstArg}
	case "TO_DECIMAL":
		return map[string]any{"$toDecimal": firstArg}
	case "PERCENTAGE":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{
			"$multiply": []any{
				map[string]any{"$divide": []any{firstArg, c.formatMongoOperand(operands[1], ast)}},
				100,
			},
		}
	case "DISCOUNT":
		if len(operands) < 2 {
			return nil
		}
		return map[string]any{
			"$subtract": []any{
				firstArg,
				map[string]any{
					"$multiply": []any{
						firstArg,
						map[string]any{"$divide": []any{c.formatMongoOperand(operands[1], ast), 100}},
					},
				},
			},
		}
	default:
		return firstArg
	}
}

// CompileDataSet compiles QueryAST into MongoDB JSON pipeline.
//
// Purpose:
// Compiles an engine QueryAST and DataSet definition into a MongoDB-specific CompiledPipeline.
//
// Where it is used:
// Invoked directly on MongoAdapter or by external callers requiring MongoDB pipeline compilation.
//
// When can it be used:
// Can be used whenever an application holds a MongoAdapter instance and needs to compile a dataset.
//
// Parameters:
//   - ctx (context.Context): Input value used by the operation.
//   - ast (*planner.QueryAST): Input value used by the operation.
//   - ds (*domain.DataSet): Input value used by the operation.
//
// Returns:
//   - result1 (*compiler.CompiledPipeline): Returned value.
//   - result2 (error): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by CompileDataSet and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - Returns validation, configuration, context, or underlying dependency errors.
//
// Note:
//   - The context controls cancellation and request lifetime.
func (a *MongoAdapter) CompileDataSet(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	return NewMongoDataSetCompiler().Compile(ctx, ast, ds)
}

// DataSetCompiler returns the adapter.DataSetCompiler instance.
//
// Purpose:
// Returns the generic adapter.DataSetCompiler interface wrapper for registering with DataSetService.
//
// Where it is used:
// In application bootstrap and dependency injection (e.g. service.RegisterCompiler("mongodb", mongoAdapter.DataSetCompiler())).
//
// When can it be used:
// Can be used when initializing the dataset engine and registering MongoDB aggregation compiler capabilities.
//
// Parameters:
//   - None.
//
// Returns:
//   - result1 (adapter.DataSetCompiler): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by DataSetCompiler and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - This function does not return an error.
//
// Note:
//   - Callers should preserve the documented ownership and lifecycle of supplied values.
func (a *MongoAdapter) DataSetCompiler() adapter.DataSetCompiler {
	return &genericCompilerWrapper{c: NewMongoDataSetCompiler()}
}

type genericCompilerWrapper struct {
	c compiler.DataSetCompiler
}

// Compile adapts generic untyped arguments to typed AST and DataSet compiler calls.
//
// Purpose:
// Unpacks generic any interface values into *planner.QueryAST and *domain.DataSet and delegates to the underlying compiler.
//
// Where it is used:
// Called dynamically by DataSetService.Preview and DataSetService.Save via the adapter.DataSetCompiler interface.
//
// When can it be used:
// Can be used whenever compiling across module boundaries where interface{} decoupling is employed.
//
// Parameters:
//   - ctx (context.Context): Input value used by the operation.
//   - ast (any): Input value used by the operation.
//   - ds (any): Input value used by the operation.
//
// Returns:
//   - result1 (any): Returned value.
//   - result2 (error): Returned value.
//
// Business Rules:
//   - Follows the invariants documented by Compile and its owning package.
//
// Side Effects:
//   - Does not intentionally mutate caller-owned input; configured dependencies may still be consulted.
//
// Errors:
//   - Returns validation, configuration, context, or underlying dependency errors.
//
// Note:
//   - The context controls cancellation and request lifetime.
func (w *genericCompilerWrapper) Compile(ctx context.Context, ast any, ds any) (any, error) {
	qAst, _ := ast.(*planner.QueryAST)
	dSet, _ := ds.(*domain.DataSet)
	return w.c.Compile(ctx, qAst, dSet)
}

// MongoStructureCategory classifies MongoDB field structures.
type MongoStructureCategory int

const (
	MongoStructureScalar MongoStructureCategory = iota
	MongoStructureArray
	MongoStructureJSONArray
	MongoStructureJSONObject
	MongoStructureJSONArrayOfObjects
)

// classifyMongoStructure determines the structure category and base type for a MongoDB column/field.
func classifyMongoStructure(dataType string) (MongoStructureCategory, string) {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	if dt == "" {
		return MongoStructureScalar, ""
	}

	// 1. Array of Objects
	if strings.Contains(dt, "OBJECT[]") || strings.Contains(dt, "ARRAY_OF_OBJECTS") ||
		strings.Contains(dt, "JSON_ARRAY_OF_OBJECTS") || dt == "[{}]" {
		return MongoStructureJSONArrayOfObjects, "OBJECT"
	}

	// 2. JSON Array of scalars
	if dt == "JSON_ARRAY" || dt == "JSONB_ARRAY" || dt == "JSON[]" || dt == "JSONB[]" || dt == "[]" {
		return MongoStructureJSONArray, "STRING"
	}

	// 3. Object
	if dt == "JSON_OBJECT" || dt == "JSONB_OBJECT" || dt == "{}" || dt == "JSON" || dt == "JSONB" || dt == "CUSTOM_TYPE" || dt == "OBJECT" {
		return MongoStructureJSONObject, "OBJECT"
	}

	// 4. Native Array (STRING[], ARRAY, etc.)
	if strings.HasSuffix(dt, "[]") || strings.HasPrefix(dt, "_") || dt == "ARRAY" {
		base := strings.TrimSuffix(dt, "[]")
		base = strings.TrimPrefix(base, "_")
		if base == "" || base == "ARRAY" {
			base = "STRING"
		}
		return MongoStructureArray, base
	}

	// 5. Scalar
	return MongoStructureScalar, dt
}

// enrichASTJoinTypes introspects selected list and projections to populate join column data types.
func enrichASTJoinTypes(ast *planner.QueryAST, ds *domain.DataSet) {
	if ast == nil {
		return
	}
	for i := range ast.Joins {
		j := &ast.Joins[i]
		if j.FromFieldDataType == "" {
			j.FromFieldDataType = lookupFieldDataType(j.FromTable, j.FromField, ast, ds)
		}
		if j.ToFieldDataType == "" {
			j.ToFieldDataType = lookupFieldDataType(j.ToTable, j.ToField, ast, ds)
			if j.ToFieldDataType == "" {
				j.ToFieldDataType = lookupFieldDataType(j.Alias, j.ToField, ast, ds)
			}
		}
	}
}

func lookupFieldDataType(table, field string, ast *planner.QueryAST, ds *domain.DataSet) string {
	tblClean := strings.ToLower(table)
	if idx := strings.Index(tblClean, "."); idx >= 0 {
		tblClean = tblClean[idx+1:]
	}
	fldLower := strings.ToLower(field)

	// Check SelectedList
	if ds != nil {
		for _, s := range ds.SelectedList {
			sField := strings.ToLower(s.Field)
			if s.DataType != "" {
				if sField == tblClean+"."+fldLower || sField == fldLower || strings.EqualFold(s.HeaderName, field) {
					return s.DataType
				}
			}
		}
	}

	// Check Projections
	if ast != nil {
		for _, p := range ast.Projections {
			pTable := strings.ToLower(p.SourceTable)
			if idx := strings.Index(pTable, "."); idx >= 0 {
				pTable = pTable[idx+1:]
			}
			if (pTable == tblClean || pTable == strings.ToLower(table)) && strings.EqualFold(p.SourceField, field) {
				if p.DataType != "" {
					return p.DataType
				}
			}
		}
	}

	// Heuristics
	if fldLower == "id" || strings.HasSuffix(fldLower, "_id") || fldLower == "_id" {
		return "STRING"
	}
	if strings.Contains(fldLower, "array") || strings.Contains(fldLower, "plant_mine") || strings.Contains(fldLower, "locations") || strings.Contains(fldLower, "days") || strings.Contains(fldLower, "tags") {
		return "ARRAY"
	}
	if strings.HasSuffix(fldLower, "_at") || strings.HasSuffix(fldLower, "_date") || strings.Contains(fldLower, "date") {
		return "DATE"
	}
	if strings.HasPrefix(fldLower, "is_") || strings.HasPrefix(fldLower, "has_") {
		return "BOOLEAN"
	}

	return ""
}

// buildMongoJoinLookup constructs the $lookup stage for a joined collection.
func (c *MongoDataSetCompiler) buildMongoJoinLookup(j planner.ASTJoin) map[string]any {
	castMode := strings.ToUpper(strings.TrimSpace(j.CastMode))
	relOp := strings.ToUpper(strings.TrimSpace(j.RelationshipOp))

	fromCat, _ := classifyMongoStructure(j.FromFieldDataType)
	toCat, _ := classifyMongoStructure(j.ToFieldDataType)

	fromIsArray := fromCat == MongoStructureArray || fromCat == MongoStructureJSONArray || fromCat == MongoStructureJSONArrayOfObjects ||
		castMode == "ANY_FROM" || castMode == "ANY_BOTH" || relOp == "ANY_FROM" || relOp == "ANY_BOTH"
	toIsArray := toCat == MongoStructureArray || toCat == MongoStructureJSONArray || toCat == MongoStructureJSONArrayOfObjects ||
		castMode == "ANY_TO" || castMode == "ANY_BOTH" || relOp == "ANY_TO" || relOp == "ANY_BOTH"

	// Auto-correct inverted ANY_TO when source is array or target is scalar PK "id" / "_id"
	if (castMode == "ANY_TO" || relOp == "ANY_TO") && (fromCat == MongoStructureArray || fromCat == MongoStructureJSONArray || strings.EqualFold(j.ToField, "id") || strings.EqualFold(j.ToField, "_id") || strings.Contains(strings.ToLower(j.FromField), "array") || strings.Contains(strings.ToLower(j.FromField), "plant_mine")) {
		fromIsArray = true
		toIsArray = false
		if castMode == "ANY_TO" {
			castMode = "ANY_FROM"
		}
		if relOp == "ANY_TO" {
			relOp = "ANY_FROM"
		}
	}

	// Fast path: standard equality without string conversion, no extra filter, no array operations
	if !j.ConvertString && len(j.JoinFilter) == 0 && (relOp == "" || relOp == "=" || relOp == "EQ" || relOp == "EQUAL") && (castMode == "" || castMode == "BOTH") && !fromIsArray && !toIsArray {
		return map[string]any{
			"$lookup": map[string]any{
				"from":         j.ToTable,
				"localField":   j.FromField,
				"foreignField": j.ToField,
				"as":           j.Alias,
			},
		}
	}

	// Pipeline $lookup
	var matchExpr map[string]any

	if (fromIsArray && toIsArray) || castMode == "ANY_BOTH" || relOp == "ANY_BOTH" || relOp == "OVERLAPS" {
		// 1. Array Overlap (Both are arrays): $gt: [ { $size: { $setIntersection: [localArr, foreignArr] } }, 0 ]
		var localExpr any = map[string]any{"$ifNull": []any{"$$localVal", []any{}}}
		var foreignExpr any = map[string]any{"$ifNull": []any{fmt.Sprintf("$%s", j.ToField), []any{}}}

		if j.ConvertString {
			if castMode != "TO_ONLY" {
				localExpr = map[string]any{
					"$map": map[string]any{
						"input": map[string]any{"$ifNull": []any{"$$localVal", []any{}}},
						"as":    "el",
						"in":    map[string]any{"$toString": "$$el"},
					},
				}
			}
			if castMode != "FROM_ONLY" {
				foreignExpr = map[string]any{
					"$map": map[string]any{
						"input": map[string]any{"$ifNull": []any{fmt.Sprintf("$%s", j.ToField), []any{}}},
						"as":    "el",
						"in":    map[string]any{"$toString": "$$el"},
					},
				}
			}
		}

		matchExpr = map[string]any{
			"$gt": []any{
				map[string]any{
					"$size": map[string]any{
						"$setIntersection": []any{localExpr, foreignExpr},
					},
				},
				0,
			},
		}
	} else if fromIsArray && !toIsArray {
		// 2. Source is Array, Target is Scalar: $in: [ foreignScalar, localArr ]
		var localExpr any = map[string]any{"$ifNull": []any{"$$localVal", []any{}}}
		var foreignExpr any = fmt.Sprintf("$%s", j.ToField)

		if j.ConvertString {
			if castMode != "TO_ONLY" {
				localExpr = map[string]any{
					"$map": map[string]any{
						"input": map[string]any{"$ifNull": []any{"$$localVal", []any{}}},
						"as":    "el",
						"in":    map[string]any{"$toString": "$$el"},
					},
				}
			}
			if castMode != "FROM_ONLY" {
				foreignExpr = map[string]any{"$toString": fmt.Sprintf("$%s", j.ToField)}
			}
		}

		matchExpr = map[string]any{
			"$in": []any{foreignExpr, localExpr},
		}
	} else if !fromIsArray && toIsArray {
		// 3. Target is Array, Source is Scalar: $in: [ localScalar, foreignArr ]
		var localExpr any = "$$localVal"
		var foreignExpr any = map[string]any{"$ifNull": []any{fmt.Sprintf("$%s", j.ToField), []any{}}}

		if j.ConvertString {
			if castMode != "TO_ONLY" {
				localExpr = map[string]any{"$toString": "$$localVal"}
			}
			if castMode != "FROM_ONLY" {
				foreignExpr = map[string]any{
					"$map": map[string]any{
						"input": map[string]any{"$ifNull": []any{fmt.Sprintf("$%s", j.ToField), []any{}}},
						"as":    "el",
						"in":    map[string]any{"$toString": "$$el"},
					},
				}
			}
		}

		matchExpr = map[string]any{
			"$in": []any{localExpr, foreignExpr},
		}
	} else {
		// 4. Scalar vs Scalar: $eq (or $ne)
		var localExpr any = "$$localVal"
		var foreignExpr any = fmt.Sprintf("$%s", j.ToField)

		if j.ConvertString {
			switch castMode {
			case "FROM_ONLY":
				localExpr = map[string]any{"$toString": "$$localVal"}
			case "TO_ONLY":
				foreignExpr = map[string]any{"$toString": fmt.Sprintf("$%s", j.ToField)}
			default:
				localExpr = map[string]any{"$toString": "$$localVal"}
				foreignExpr = map[string]any{"$toString": fmt.Sprintf("$%s", j.ToField)}
			}
		}

		op := "$eq"
		if relOp == "<>" || relOp == "!=" || relOp == "NEQ" || relOp == "NOT_EQUAL" {
			op = "$ne"
		}
		matchExpr = map[string]any{
			op: []any{foreignExpr, localExpr},
		}
	}

	pipeline := []map[string]any{
		{
			"$match": map[string]any{
				"$expr": matchExpr,
			},
		},
	}

	if len(j.JoinFilter) > 0 {
		pipeline = append(pipeline, map[string]any{"$match": j.JoinFilter})
	}

	return map[string]any{
		"$lookup": map[string]any{
			"from":     j.ToTable,
			"let":      map[string]any{"localVal": fmt.Sprintf("$%s", j.FromField)},
			"pipeline": pipeline,
			"as":       j.Alias,
		},
	}
}

// setProjectField sets a projection field while avoiding MongoDB path collisions.
// In MongoDB $project, a parent field path (e.g. "delegates") and its subfield path (e.g. "delegates.time")
// cannot both exist in the same $project stage (error Location31249: Path collision).
func setProjectField(projectStage map[string]any, key string, val any) {
	if key == "" {
		return
	}
	// 1. If an ancestor of 'key' is already in projectStage, 'key' is already covered by that ancestor.
	for existingKey := range projectStage {
		if strings.HasPrefix(key, existingKey+".") {
			return
		}
	}

	// 2. If 'key' is an ancestor of any existing keys, remove those descendant keys.
	for existingKey := range projectStage {
		if strings.HasPrefix(existingKey, key+".") {
			delete(projectStage, existingKey)
		}
	}

	// 3. Set the key
	projectStage[key] = val
}

