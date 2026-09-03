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
		if cond.Table == ast.BaseTable.Table || cond.Table == ast.BaseTable.Alias {
			if cond.IsParamRef && parameterized {
				matchStage[cond.Column] = map[string]any{
					"$eq": map[string]any{
						"ParamsName":    cond.ParamName,
						"parmsDataType": cond.ParamDataType,
					},
				}
			} else if cond.IsParamRef && !parameterized {
				for _, p := range ast.Parameters {
					if strings.EqualFold(p.ParamName, cond.ParamName) && p.DefaultValue != nil {
						matchStage[cond.Column] = p.DefaultValue
						break
					}
				}
			} else if cond.Value != nil {
				matchStage[cond.Column] = cond.Value
			}
		}
	}

	if len(matchStage) > 0 {
		stages = append(stages, map[string]any{"$match": matchStage})
	}

	// 2. $lookup stages for joins
	for _, j := range ast.Joins {
		lookupStage := map[string]any{
			"$lookup": map[string]any{
				"from":         j.ToTable,
				"localField":   j.FromField,
				"foreignField": j.ToField,
				"as":           j.Alias,
			},
		}
		stages = append(stages, lookupStage)
	}

	// 3. $group stage if GroupBy or Aggregates are present
	if len(ast.GroupBy) > 0 {
		groupStage := map[string]any{}
		if len(ast.GroupBy) == 1 {
			groupStage["_id"] = fmt.Sprintf("$%s", ast.GroupBy[0].Field)
		} else {
			idMap := make(map[string]any)
			for _, g := range ast.GroupBy {
				idMap[g.Field] = fmt.Sprintf("$%s", g.Field)
			}
			groupStage["_id"] = idMap
		}

		for _, cc := range ast.CustomColumns {
			if cc.IsAggregate && cc.Function != nil {
				opField := ""
				if len(cc.Operands) > 0 {
					opField = fmt.Sprintf("$%s", cc.Operands[0].SourceField)
				}
				switch strings.ToUpper(cc.Function.Name) {
				case "SUM":
					groupStage[cc.Alias] = map[string]any{"$sum": opField}
				case "AVG":
					groupStage[cc.Alias] = map[string]any{"$avg": opField}
				case "MIN":
					groupStage[cc.Alias] = map[string]any{"$min": opField}
				case "MAX":
					groupStage[cc.Alias] = map[string]any{"$max": opField}
				case "COUNT":
					groupStage[cc.Alias] = map[string]any{"$sum": 1}
				}
			}
		}

		stages = append(stages, map[string]any{"$group": groupStage})
	}

	// 4. $project stage
	projectStage := make(map[string]any)
	for _, p := range ast.Projections {
		projectStage[p.SourceField] = 1
	}
	for _, cc := range ast.CustomColumns {
		if !cc.IsAggregate {
			projectStage[cc.Alias] = 1
		}
	}

	if len(projectStage) > 0 {
		stages = append(stages, map[string]any{"$project": projectStage})
	}

	bytes, _ := json.Marshal(stages)
	return string(bytes)
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
