# models-go-mongodb

> **MongoDB Aggregation Pipeline Compiler & Document Store Adapter**

`models-go-mongodb` provides the MongoDB adapter for `models-go-engine`. It compiles relational Query AST into MongoDB `$lookup`, `$match`, `$project`, `$unwind`, and `$group` aggregation pipelines.

## Prerequisites

- Go 1.20 or newer.
- MongoDB 6 or newer, reachable using a MongoDB URI.
- A database and credentials with permission to read and write the target collections.

## Usage Steps

1. Set the MongoDB URI and database name in application configuration.
2. Create the adapter with `mongodb.NewMongoAdapter(uri, database)`.
3. Call `Connect(ctx)` and handle the connection error.
4. Register the adapter/compiler with `models-go-engine`.
5. Run queries, CRUD operations, or dataset preview/execute operations.
6. Run `go test ./...` before committing changes.

## Core Functionality

- MongoDB connection and execution lifecycle.
- Relational joins compiled to `$lookup` pipelines.
- Filtering, projection, grouping, and array/object relation shaping.
- Common engine execution and dataset compiler interfaces.

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

### Selecting fields inside dataset relations

`OBJECT` joins retain one projected subdocument. `MANY`/`JSON_ARRAY` joins retain the `$lookup` result as an array and use `$map` to include only relation fields present in `selected_list`. `NONE` keeps the existing flat projection behavior.

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
