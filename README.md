# models-go-mongodb

> **MongoDB Aggregation Pipeline Compiler & Document Store Adapter**

`models-go-mongodb` provides the MongoDB adapter for `models-go-engine`. It compiles relational Query AST into MongoDB `$lookup`, `$match`, `$project`, `$unwind`, and `$group` aggregation pipelines.

---

## 🛠️ Key Exported Functions & Methods Reference

### `MongoAdapter` ([`adapter.go`](./adapter.go))

| Function / Method | Signature | Description |
| :--- | :--- | :--- |
| `NewMongoAdapter` | `(uri, database string) *MongoAdapter` | Creates a new MongoDB adapter instance. |
| `Connect` | `(ctx context.Context) error` | Establishes connection to MongoDB database cluster. |
| `Execute` | `(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error)` | Runs aggregation pipelines or document CRUD operations. |
| `CompileDataSet` | `(ctx context.Context, ast *planner.QueryAST, ds *domain.DataSet) (*compiler.CompiledPipeline, error)` | Compiles Query AST into JSON MongoDB aggregation pipeline array. |

---

## 🚀 Usage Example

```go
package main

import (
	"context"
	"fmt"

	"github.com/SanjayDrop5528/models-go-mongodb"
)

func main() {
	adapter := mongodb.NewMongoAdapter("mongodb://localhost:27017", "app_store")
	if err := adapter.Connect(context.Background()); err != nil {
		panic(err)
	}

	fmt.Println("Connected to Mongo Adapter:", adapter.Name())
}
```
