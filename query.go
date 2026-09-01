package mongodb

import (
	"fmt"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"strings"
)

// QueryBuilder translates query.Query into MongoDB BSON filter queries.
type QueryBuilder struct{}

// BuildFilter translates query filters to MongoDB filter documents.
func (b *QueryBuilder) BuildFilter(q query.Query) map[string]any {
	if len(q.Filters) == 0 {
		return map[string]any{}
	}

	filterDoc := make(map[string]any)
	var orClauses []map[string]any

	for _, f := range q.Filters {
		cond := make(map[string]any)

		switch f.Op {
		case query.OpEq:
			cond = map[string]any{f.Field: f.Value}
		case query.OpNeq:
			cond = map[string]any{f.Field: map[string]any{"$ne": f.Value}}
		case query.OpGt:
			cond = map[string]any{f.Field: map[string]any{"$gt": f.Value}}
		case query.OpGte:
			cond = map[string]any{f.Field: map[string]any{"$gte": f.Value}}
		case query.OpLt:
			cond = map[string]any{f.Field: map[string]any{"$lt": f.Value}}
		case query.OpLte:
			cond = map[string]any{f.Field: map[string]any{"$lte": f.Value}}
		case query.OpIn:
			cond = map[string]any{f.Field: map[string]any{"$in": f.Value}}
		case query.OpNin:
			cond = map[string]any{f.Field: map[string]any{"$nin": f.Value}}
		case query.OpLike, query.OpILike:
			pat := strings.ReplaceAll(fmt.Sprintf("%v", f.Value), "%", ".*")
			opts := ""
			if f.Op == query.OpILike {
				opts = "i"
			}
			cond = map[string]any{f.Field: map[string]any{"$regex": pat, "$options": opts}}
		case query.OpNotLike:
			pat := strings.ReplaceAll(fmt.Sprintf("%v", f.Value), "%", ".*")
			cond = map[string]any{f.Field: map[string]any{"$not": map[string]any{"$regex": pat, "$options": "i"}}}
		case query.OpBetween:
			cond = map[string]any{f.Field: map[string]any{"$gte": f.Value, "$lte": f.ValueTo}}
		case query.OpIsNull:
			cond = map[string]any{f.Field: nil}
		case query.OpIsNotNull:
			cond = map[string]any{f.Field: map[string]any{"$ne": nil}}
		default:
			cond = map[string]any{f.Field: f.Value}
		}

		if q.LogicalOp == query.OpOr {
			orClauses = append(orClauses, cond)
		} else {
			for k, v := range cond {
				filterDoc[k] = v
			}
		}
	}

	if q.LogicalOp == query.OpOr && len(orClauses) > 0 {
		return map[string]any{"$or": orClauses}
	}

	return filterDoc
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
