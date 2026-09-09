package mongodb

import (
	"fmt"
	"strings"

	"github.com/SanjayDrop5528/models-go-engine/query"
)

// QueryBuilder translates unified query.Query into MongoDB BSON filter queries and aggregation pipelines.
type QueryBuilder struct{}

// BuildFilter translates query filters, raw expressions, and where groups to MongoDB filter documents.
func (b *QueryBuilder) BuildFilter(q query.Query) map[string]any {
	filterDoc := make(map[string]any)
	var andClauses []map[string]any
	var orClauses []map[string]any

	// 1. Standard structured filters
	for _, f := range q.Filters {
		cond := b.buildSingleFilter(f)
		if q.LogicalOp == query.OpOr {
			orClauses = append(orClauses, cond)
		} else {
			for k, v := range cond {
				filterDoc[k] = v
			}
		}
	}

	// 2. Raw where expressions (e.g., "status = ?", "age >= ?")
	for _, rw := range q.RawWheres {
		cond := b.parseRawWhere(rw)
		if len(cond) > 0 {
			if q.LogicalOp == query.OpOr {
				orClauses = append(orClauses, cond)
			} else {
				for k, v := range cond {
					filterDoc[k] = v
				}
			}
		}
	}

	// 3. Where groups (nested parenthesized condition groups)
	for _, wg := range q.WhereGroups {
		subDoc := b.BuildFilter(wg.Query)
		if len(subDoc) > 0 {
			if strings.Contains(wg.Sep, "OR") {
				orClauses = append(orClauses, subDoc)
			} else {
				andClauses = append(andClauses, subDoc)
			}
		}
	}

	if q.LogicalOp == query.OpOr && len(orClauses) > 0 {
		return map[string]any{"$or": orClauses}
	}

	if len(andClauses) > 0 {
		if len(filterDoc) > 0 {
			andClauses = append(andClauses, filterDoc)
		}
		return map[string]any{"$and": andClauses}
	}

	return filterDoc
}

func (b *QueryBuilder) buildSingleFilter(f query.Filter) map[string]any {
	switch f.Op {
	case query.OpEq:
		return map[string]any{f.Field: f.Value}
	case query.OpNeq:
		return map[string]any{f.Field: map[string]any{"$ne": f.Value}}
	case query.OpGt:
		return map[string]any{f.Field: map[string]any{"$gt": f.Value}}
	case query.OpGte:
		return map[string]any{f.Field: map[string]any{"$gte": f.Value}}
	case query.OpLt:
		return map[string]any{f.Field: map[string]any{"$lt": f.Value}}
	case query.OpLte:
		return map[string]any{f.Field: map[string]any{"$lte": f.Value}}
	case query.OpIn:
		return map[string]any{f.Field: map[string]any{"$in": f.Value}}
	case query.OpNin:
		return map[string]any{f.Field: map[string]any{"$nin": f.Value}}
	case query.OpLike, query.OpILike:
		pat := strings.ReplaceAll(fmt.Sprintf("%v", f.Value), "%", ".*")
		opts := ""
		if f.Op == query.OpILike {
			opts = "i"
		}
		return map[string]any{f.Field: map[string]any{"$regex": pat, "$options": opts}}
	case query.OpNotLike:
		pat := strings.ReplaceAll(fmt.Sprintf("%v", f.Value), "%", ".*")
		return map[string]any{f.Field: map[string]any{"$not": map[string]any{"$regex": pat, "$options": "i"}}}
	case query.OpBetween:
		return map[string]any{f.Field: map[string]any{"$gte": f.Value, "$lte": f.ValueTo}}
	case query.OpIsNull:
		return map[string]any{f.Field: nil}
	case query.OpIsNotNull:
		return map[string]any{f.Field: map[string]any{"$ne": nil}}
	default:
		return map[string]any{f.Field: f.Value}
	}
}

func (b *QueryBuilder) parseRawWhere(rw query.RawExpr) map[string]any {
	q := strings.TrimSpace(rw.Query)
	parts := strings.Fields(q)
	if len(parts) == 0 {
		return nil
	}

	field := parts[0]
	if len(parts) >= 3 && parts[1] == "=" && len(rw.Args) > 0 {
		return map[string]any{field: rw.Args[0]}
	}
	if len(parts) >= 3 && parts[1] == ">" && len(rw.Args) > 0 {
		return map[string]any{field: map[string]any{"$gt": rw.Args[0]}}
	}
	if len(parts) >= 3 && parts[1] == ">=" && len(rw.Args) > 0 {
		return map[string]any{field: map[string]any{"$gte": rw.Args[0]}}
	}
	if len(parts) >= 3 && parts[1] == "<" && len(rw.Args) > 0 {
		return map[string]any{field: map[string]any{"$lt": rw.Args[0]}}
	}
	if len(parts) >= 3 && parts[1] == "<=" && len(rw.Args) > 0 {
		return map[string]any{field: map[string]any{"$lte": rw.Args[0]}}
	}
	if len(parts) >= 3 && (parts[1] == "!=" || parts[1] == "<>") && len(rw.Args) > 0 {
		return map[string]any{field: map[string]any{"$ne": rw.Args[0]}}
	}

	if len(rw.Args) > 0 {
		return map[string]any{field: rw.Args[0]}
	}
	return nil
}

// BuildSort translates sorts into MongoDB sort documents.
func (b *QueryBuilder) BuildSort(sorts []query.Sort) map[string]int {
	sortDoc := make(map[string]int)
	for _, s := range sorts {
		if s.Order == query.SortDesc {
			sortDoc[s.Field] = -1
		} else {
			sortDoc[s.Field] = 1
		}
	}
	return sortDoc
}

// BuildPipeline translates the unified Query into a full MongoDB aggregation pipeline
// supporting $match, $lookup (relations/joins), $project, $sort, $skip, $limit, and $unionWith.
func (b *QueryBuilder) BuildPipeline(q query.Query) []map[string]any {
	var pipeline []map[string]any

	// 1. $match stage
	filterDoc := b.BuildFilter(q)
	if len(filterDoc) > 0 {
		pipeline = append(pipeline, map[string]any{"$match": filterDoc})
	}

	// 2. $lookup for Joins
	for _, j := range q.Joins {
		pipeline = append(pipeline, map[string]any{
			"$lookup": map[string]any{
				"from":         j.Table,
				"localField":   "id",
				"foreignField": strings.TrimSuffix(strings.TrimPrefix(j.Table, "join_"), "s") + "_id",
				"as":           j.Table,
			},
		})
	}

	// 3. $lookup for Relations (eager child loading)
	for _, rel := range q.Relations {
		pipeline = append(pipeline, map[string]any{
			"$lookup": map[string]any{
				"from":         strings.ToLower(rel),
				"localField":   "id",
				"foreignField": strings.ToLower(rel) + "_id",
				"as":           rel,
			},
		})
	}

	// 4. $unionWith for Unions
	for _, u := range q.Unions {
		if u.Query != nil {
			targetTable := ""
			if len(u.Query.Tables) > 0 {
				targetTable = u.Query.Tables[0]
			}
			unionPipeline := b.BuildPipeline(*u.Query)
			pipeline = append(pipeline, map[string]any{
				"$unionWith": map[string]any{
					"coll":     targetTable,
					"pipeline": unionPipeline,
				},
			})
		}
	}

	// 5. $project for Fields & ExcludedColumns
	if len(q.Fields) > 0 || len(q.ExcludedColumns) > 0 {
		projectDoc := make(map[string]any)
		for _, f := range q.Fields {
			projectDoc[f] = 1
		}
		for _, ef := range q.ExcludedColumns {
			projectDoc[ef] = 0
		}
		if len(projectDoc) > 0 {
			pipeline = append(pipeline, map[string]any{"$project": projectDoc})
		}
	}

	// 6. $sort stage
	if len(q.Sorts) > 0 {
		sortDoc := b.BuildSort(q.Sorts)
		pipeline = append(pipeline, map[string]any{"$sort": sortDoc})
	}

	// 7. $skip stage
	if q.Pagination.Offset > 0 {
		pipeline = append(pipeline, map[string]any{"$skip": q.Pagination.Offset})
	}

	// 8. $limit stage
	if q.Pagination.Limit > 0 {
		pipeline = append(pipeline, map[string]any{"$limit": q.Pagination.Limit})
	}

	return pipeline
}
