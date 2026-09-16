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
func NewMongoDataSetCompiler() *MongoDataSetCompiler {
	return &MongoDataSetCompiler{}
}

// Compile compiles the QueryAST into MongoDB JSON aggregation pipeline.
func (c *MongoDataSetCompiler) Compile(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	if ast == nil {
		return nil, domain.NewError(domain.ErrPipelineCompilationFailed, "cannot compile nil AST")
	}

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
		var lookupStage map[string]any
		if j.ConvertString {
			var localExpr any = "$$localVal"
			var foreignExpr any = fmt.Sprintf("$%s", j.ToField)

			switch strings.ToUpper(j.CastMode) {
			case "FROM_ONLY":
				localExpr = map[string]any{"$toString": "$$localVal"}
			case "TO_ONLY":
				foreignExpr = map[string]any{"$toString": fmt.Sprintf("$%s", j.ToField)}
			default:
				localExpr = map[string]any{"$toString": "$$localVal"}
				foreignExpr = map[string]any{"$toString": fmt.Sprintf("$%s", j.ToField)}
			}

			pipeline := []map[string]any{
				{
					"$match": map[string]any{
						"$expr": map[string]any{
							"$eq": []any{foreignExpr, localExpr},
						},
					},
				},
			}

			if len(j.JoinFilter) > 0 {
				pipeline = append(pipeline, map[string]any{"$match": j.JoinFilter})
			}

			lookupStage = map[string]any{
				"$lookup": map[string]any{
					"from": j.ToTable,
					"let":  map[string]any{"localVal": fmt.Sprintf("$%s", j.FromField)},
					"pipeline": pipeline,
					"as": j.Alias,
				},
			}
		} else {
			if len(j.JoinFilter) > 0 {
				lookupStage = map[string]any{
					"$lookup": map[string]any{
						"from": j.ToTable,
						"let":  map[string]any{"localVal": fmt.Sprintf("$%s", j.FromField)},
						"pipeline": []map[string]any{
							{
								"$match": map[string]any{
									"$expr": map[string]any{
										"$eq": []any{fmt.Sprintf("$%s", j.ToField), "$$localVal"},
									},
								},
							},
							{
								"$match": j.JoinFilter,
							},
						},
						"as": j.Alias,
					},
				}
			} else {
				lookupStage = map[string]any{
					"$lookup": map[string]any{
						"from":         j.ToTable,
						"localField":   j.FromField,
						"foreignField": j.ToField,
						"as":           j.Alias,
					},
				}
			}
		}
		stages = append(stages, lookupStage)

		// Unwind joined subdocuments so fields are directly accessible
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
			postGroupProject[ast.GroupBy[0].Field] = "$_id"
		} else if len(ast.GroupBy) > 1 {
			for _, g := range ast.GroupBy {
				postGroupProject[g.Field] = fmt.Sprintf("$_id.%s", g.Field)
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
					postGroupProject[cc.Alias] = map[string]any{"$size": fmt.Sprintf("$%s_set", cc.Alias)}
				} else {
					postGroupProject[cc.Alias] = 1
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
			alias := p.Alias
			if alias == "" {
				alias = p.SourceField
			}
			if alias == "_id" {
				hasExplicitID = true
			}

			path := c.resolveFieldRef(p.SourceTable, p.SourceField, ast)
			if alias == strings.TrimPrefix(path, "$") {
				projectStage[alias] = 1
			} else {
				projectStage[alias] = path
			}
		}

		for _, cc := range ast.CustomColumns {
			if cc.Alias == "_id" {
				hasExplicitID = true
			}
			projectStage[cc.Alias] = 1
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

func (c *MongoDataSetCompiler) resolveFieldRef(table, field string, ast *planner.QueryAST) string {
	return "$" + c.resolveFieldPath(table, field, ast)
}

func (c *MongoDataSetCompiler) formatMongoOperand(op planner.ASTOperand, ast *planner.QueryAST) any {
	if op.IsLiteral || op.SourceTable == "_LITERAL_" {
		return op.LiteralVal
	}
	if op.SourceTable == "" || op.SourceTable == "CALC" {
		return "$" + op.SourceField
	}
	return c.resolveFieldRef(op.SourceTable, op.SourceField, ast)
}

func (c *MongoDataSetCompiler) applyFilterCondition(matchStage map[string]any, path string, cond planner.ASTCondition, parameterized bool, params []domain.FilterParam) {
	var val any
	if cond.IsParamRef {
		if parameterized {
			val = map[string]any{
				"ParamsName":    cond.ParamName,
				"parmsDataType": cond.ParamDataType,
			}
		} else {
			for _, p := range params {
				if strings.EqualFold(p.ParamName, cond.ParamName) && p.DefaultValue != nil {
					val = p.DefaultValue
					break
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
func (a *MongoAdapter) CompileDataSet(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error) {
	return NewMongoDataSetCompiler().Compile(ctx, ast, ds)
}

// DataSetCompiler returns the adapter.DataSetCompiler instance.
func (a *MongoAdapter) DataSetCompiler() adapter.DataSetCompiler {
	return &genericCompilerWrapper{c: NewMongoDataSetCompiler()}
}

type genericCompilerWrapper struct {
	c compiler.DataSetCompiler
}

func (w *genericCompilerWrapper) Compile(ctx context.Context, ast any, ds any) (any, error) {
	qAst, _ := ast.(*planner.QueryAST)
	dSet, _ := ds.(*domain.DataSet)
	return w.c.Compile(ctx, qAst, dSet)
}
