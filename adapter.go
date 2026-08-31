package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"github.com/SanjayDrop5528/models-go-engine/adapter"
	"github.com/SanjayDrop5528/models-go-engine/diff"
	"github.com/SanjayDrop5528/models-go-engine/execution"
	"github.com/SanjayDrop5528/models-go-engine/mapping"
	"github.com/SanjayDrop5528/models-go-engine/model"
	"github.com/SanjayDrop5528/models-go-engine/operation"
	"github.com/SanjayDrop5528/models-go-engine/plan"
	"github.com/SanjayDrop5528/models-go-engine/query"
	"github.com/SanjayDrop5528/models-go-engine/schema"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoAdapter implements the Adapter interface for MongoDB with live network and mock fallback support.
type MongoAdapter struct {
	uri          string
	database     string
	client       *mongo.Client
	queryBuilder *QueryBuilder
	mu           sync.RWMutex
	mockStore    map[string][]map[string]any // collection -> documents for standalone/testing
	mockSchemas  map[string]*schema.Schema
}

// NewMongoAdapter creates a new MongoDB adapter instance.
func NewMongoAdapter(uri, database string) *MongoAdapter {
	if database == "" {
		database = "dev"
	}
	return &MongoAdapter{
		uri:          uri,
		database:     database,
		queryBuilder: &QueryBuilder{},
		mockStore:    make(map[string][]map[string]any),
		mockSchemas:  make(map[string]*schema.Schema),
	}
}

func (a *MongoAdapter) Name() string {
	return "mongodb"
}

// Client returns the underlying *mongo.Client handle.
func (a *MongoAdapter) Client() *mongo.Client {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.client
}

// NativeClient returns the underlying *mongo.Client connection handle.
func (a *MongoAdapter) NativeClient() any {
	return a.Client()
}

// DatabaseName returns the target database name.
func (a *MongoAdapter) DatabaseName() string {
	return a.GetDatabaseName()
}

// GetDatabaseName returns the target MongoDB database name.
func (a *MongoAdapter) GetDatabaseName() string {
	if a.database != "" {
		return a.database
	}
	return "mongodb"
}

// getClient lazily and thread-safely connects to the live MongoDB Atlas cluster when a URI is provided.
func (a *MongoAdapter) getClient(ctx context.Context) (*mongo.Client, error) {
	if strings.TrimSpace(a.uri) == "" {
		return nil, nil // Offline mock fallback mode
	}

	a.mu.RLock()
	if a.client != nil {
		c := a.client
		a.mu.RUnlock()
		return c, nil
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.client != nil {
		return a.client, nil
	}

	log.Printf("[MongoDB] Connecting to live cluster at: %s (DB: %s)...", a.uri, a.database)
	clientOpts := options.Client().ApplyURI(a.uri).SetServerSelectionTimeout(10 * time.Second)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to MongoDB: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("failed to ping live MongoDB: %w", err)
	}

	log.Printf("[MongoDB] ✔ Connected successfully to live MongoDB database '%s'!", a.database)
	a.client = client
	_ = a.EnsureMetadataTables(ctx)
	return a.client, nil
}

// EnsureMetadataTables creates 'model_configs' and 'data_models' collections in MongoDB if they do not exist.
func (a *MongoAdapter) EnsureMetadataTables(ctx context.Context) error {
	client, err := a.getClient(ctx)
	if err != nil || client == nil {
		return nil
	}
	db := client.Database(a.database)
	_ = db.CreateCollection(ctx, "model_configs")
	_ = db.CreateCollection(ctx, "data_models")
	log.Printf("[MongoDB] ✔ System metadata collections ('model_configs' and 'data_models') verified & active.")
	return nil
}

// ImportLiveMetadata introspects live MongoDB collections and auto-populates model_configs & data_models.
func (a *MongoAdapter) ImportLiveMetadata(ctx context.Context) ([]*model.ModelConfig, []*model.DataModel, error) {
	client, err := a.getClient(ctx)
	if err != nil || client == nil {
		return nil, nil, err
	}
	db := client.Database(a.database)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed listing MongoDB collections: %w", err)
	}

	var configs []*model.ModelConfig
	var fields []*model.DataModel

	for _, collName := range collections {
		if collName == "model_configs" || collName == "data_models" || strings.HasPrefix(collName, "system.") {
			continue
		}

		modelID := collName
		modelName := strings.Title(collName)

		cfg := &model.ModelConfig{
			ID:                   modelID,
			Name:                 modelName,
			Table:                collName,
			RefName:              collName,
			Schema:               a.database,
			IsAttributeReference: false,
			Description:          fmt.Sprintf("Auto-imported from MongoDB collection '%s'", collName),
			Status:               model.ModelConfigStatusActive,
			Version:              1,
			CreatedAt:            time.Now(),
			UpdatedAt:            time.Now(),
		}
		configs = append(configs, cfg)

		// Sample 1 document to infer schema fields
		var sample bson.M
		err := db.Collection(collName).FindOne(ctx, bson.M{}).Decode(&sample)
		if err == nil {
			for key, val := range sample {
				fieldID := fmt.Sprintf("%s_%s", modelID, key)
				dataType := model.TypeString
				switch val.(type) {
				case int, int32, int64:
					dataType = model.TypeLong
				case float32, float64:
					dataType = model.TypeDecimal
				case bool:
					dataType = model.TypeBoolean
				case primitive.DateTime, time.Time:
					dataType = model.TypeDateTime
				case bson.A:
					dataType = model.TypeArray
				case bson.M, map[string]any:
					dataType = model.TypeJSON
				}

				isPK := (key == "_id" || key == "id")

				dm := &model.DataModel{
					ID:           fieldID,
					ModelID:      modelID,
					ColumnName:   key,
					JSONField:    key,
					DataType:     dataType,
					IsNullable:   !isPK,
					IsRequired:   isPK,
					IsPrimaryKey: isPK,
					Status:       model.DataModelStatusActive,
					CreatedAt:    time.Now(),
					UpdatedAt:    time.Now(),
				}
				fields = append(fields, dm)
			}
			log.Printf("[MongoDB] [Import Collection] Sampled Collection '%s' -> ModelConfig ID='%s', Inferred Fields=%d", collName, modelID, len(sample))
		} else {
			log.Printf("[MongoDB] [Import Collection] Discovered Collection '%s' (empty, registered 0 inferred fields)", collName)
		}
	}

	log.Printf("[MongoDB] ✔ [Import Success] Successfully introspected %d ModelConfig(s) and %d DataModel field(s) directly inside adapter.", len(configs), len(fields))
	return configs, fields, nil
}

func (a *MongoAdapter) Connect(ctx context.Context) error {
	_, err := a.getClient(ctx)
	return err
}

func (a *MongoAdapter) Ping(ctx context.Context) error {
	client, err := a.getClient(ctx)
	if err != nil {
		return err
	}
	if client != nil {
		return client.Ping(ctx, readpref.Primary())
	}
	return nil
}

func (a *MongoAdapter) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client != nil {
		err := a.client.Disconnect(ctx)
		a.client = nil
		return err
	}
	return nil
}

// GetSchema returns the collection schema including indexed fields and validator rules.
func (a *MongoAdapter) GetSchema(ctx context.Context, ref model.ModelRef) (*schema.Schema, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}

	if s, ok := a.mockSchemas[collName]; ok {
		return s, nil
	}
	return nil, nil
}

// ValidateSchemaPlan checks for MongoDB compatibility.
func (a *MongoAdapter) ValidateSchemaPlan(ctx context.Context, p *plan.SchemaPlan) error {
	return nil
}

// PreviewSchemaChange generates MongoDB commands and documents.
func (a *MongoAdapter) PreviewSchemaChange(ctx context.Context, p *plan.SchemaPlan) (*plan.SchemaPreview, error) {
	actions := make([]plan.NativeAction, 0, len(p.Operations))

	for _, op := range p.Operations {
		switch op.Type {
		case diff.OpCreateTable:
			var validatorJSON string
			if s, ok := op.After.(*schema.Schema); ok {
				valDoc := BuildJSONSchema(s)
				if b, err := json.MarshalIndent(valDoc, "", "  "); err == nil {
					validatorJSON = string(b)
				}
			}
			actions = append(actions, plan.NativeAction{
				Type:        "MONGODB_CMD",
				Description: fmt.Sprintf("Create collection '%s' with $jsonSchema validator", p.StorageName),
				Statement:   fmt.Sprintf("db.createCollection(\"%s\", { validator: %s })", p.StorageName, validatorJSON),
				Destructive: false,
			})

		case diff.OpDropTable:
			actions = append(actions, plan.NativeAction{
				Type:        "MONGODB_CMD",
				Description: fmt.Sprintf("Drop collection '%s'", p.StorageName),
				Statement:   fmt.Sprintf("db.%s.drop()", p.StorageName),
				Destructive: true,
			})

		case diff.OpAddColumn:
			actions = append(actions, plan.NativeAction{
				Type:        "MONGODB_DYNAMIC_FIELD",
				Description: fmt.Sprintf("Dynamic field '%s' accepted without physical table alteration (MongoDB flexible document model)", op.ObjectName),
				Statement:   fmt.Sprintf("// Dynamic field '%s' accepted in MongoDB collection '%s'", op.ObjectName, p.StorageName),
				Destructive: false,
			})

		case diff.OpRemoveColumn:
			actions = append(actions, plan.NativeAction{
				Type:        "MONGODB_CMD",
				Description: fmt.Sprintf("Unset field '%s' from all documents", op.ObjectName),
				Statement:   fmt.Sprintf("db.%s.updateMany({}, { $unset: { \"%s\": \"\" } })", p.StorageName, op.ObjectName),
				Destructive: true,
			})

		case diff.OpRenameColumn:
			actions = append(actions, plan.NativeAction{
				Type:        "MONGODB_CMD",
				Description: fmt.Sprintf("Rename field from '%s' to '%s' via $rename", op.OldName, op.ObjectName),
				Statement:   fmt.Sprintf("db.%s.updateMany({}, { $rename: { \"%s\": \"%s\" } })", p.StorageName, op.OldName, op.ObjectName),
				Destructive: false,
			})

		case diff.OpAddIndex:
			if idx, ok := op.After.(schema.SchemaIndex); ok {
				keys := make([]string, len(idx.Columns))
				for i, c := range idx.Columns {
					keys[i] = fmt.Sprintf("\"%s\": 1", c)
				}
				uniqueOption := ""
				if idx.Unique {
					uniqueOption = ", { unique: true }"
				}
				actions = append(actions, plan.NativeAction{
					Type:        "MONGODB_CMD",
					Description: fmt.Sprintf("Create index '%s'", idx.Name),
					Statement:   fmt.Sprintf("db.%s.createIndex({ %s }%s)", p.StorageName, strings.Join(keys, ", "), uniqueOption),
					Destructive: false,
				})
			}

		case diff.OpDropIndex:
			actions = append(actions, plan.NativeAction{
				Type:        "MONGODB_CMD",
				Description: fmt.Sprintf("Drop index '%s'", op.ObjectName),
				Statement:   fmt.Sprintf("db.%s.dropIndex(\"%s\")", p.StorageName, op.ObjectName),
				Destructive: false,
			})
		}
	}

	return &plan.SchemaPreview{
		ModelID:              p.ModelID,
		StorageName:          p.StorageName,
		Database:             "mongodb",
		Changes:              p.Operations,
		NativeActions:        actions,
		HasDestructive:       p.Destructive,
		RequiresConfirmation: p.Destructive,
		Warnings:             p.Warnings,
		Status:               "READY",
	}, nil
}

// ApplySchemaChange updates the MongoDB collections, indexes, or validator rules on live MongoDB and in memory.
func (a *MongoAdapter) ApplySchemaChange(ctx context.Context, p *plan.SchemaPlan) error {
	a.mu.Lock()
	collName := p.StorageName
	curr := a.mockSchemas[collName]

	for _, op := range p.Operations {
		switch op.Type {
		case diff.OpCreateTable:
			if s, ok := op.After.(*schema.Schema); ok {
				a.mockSchemas[collName] = s
				if _, exists := a.mockStore[collName]; !exists {
					a.mockStore[collName] = make([]map[string]any, 0)
				}
			}
		case diff.OpDropTable:
			delete(a.mockSchemas, collName)
			delete(a.mockStore, collName)
		case diff.OpAddColumn:
			if curr != nil {
				if attr, ok := op.After.(schema.SchemaAttribute); ok {
					curr.Attributes = append(curr.Attributes, attr)
				}
			}
		case diff.OpRemoveColumn:
			if curr != nil {
				var remaining []schema.SchemaAttribute
				for _, attr := range curr.Attributes {
					if attr.Name != op.ObjectName {
						remaining = append(remaining, attr)
					}
				}
				curr.Attributes = remaining
			}
			for i := range a.mockStore[collName] {
				delete(a.mockStore[collName][i], op.ObjectName)
			}
		case diff.OpRenameColumn:
			if curr != nil {
				for i := range curr.Attributes {
					if curr.Attributes[i].Name == op.OldName {
						curr.Attributes[i].Name = op.ObjectName
					}
				}
			}
			for i := range a.mockStore[collName] {
				if val, exists := a.mockStore[collName][i][op.OldName]; exists {
					a.mockStore[collName][i][op.ObjectName] = val
					delete(a.mockStore[collName][i], op.OldName)
				}
			}
		case diff.OpAddIndex:
			if curr != nil {
				if idx, ok := op.After.(schema.SchemaIndex); ok {
					curr.Indexes = append(curr.Indexes, idx)
				}
			}
		case diff.OpDropIndex:
			if curr != nil {
				var remaining []schema.SchemaIndex
				for _, idx := range curr.Indexes {
					if idx.Name != op.ObjectName {
						remaining = append(remaining, idx)
					}
				}
				curr.Indexes = remaining
			}
		}
	}
	a.mu.Unlock()

	// If live MongoDB is configured, apply collection and index changes to Atlas
	client, err := a.getClient(ctx)
	if err == nil && client != nil {
		db := client.Database(a.database)
		for _, op := range p.Operations {
			switch op.Type {
			case diff.OpCreateTable:
				names, _ := db.ListCollectionNames(ctx, bson.M{"name": collName})
				exists := false
				for _, n := range names {
					if n == collName {
						exists = true
						break
					}
				}
				if !exists {
					opts := options.CreateCollection()
					if s, ok := op.After.(*schema.Schema); ok {
						valDoc := BuildJSONSchema(s)
						opts.SetValidator(valDoc)
					}
					_ = db.CreateCollection(ctx, collName, opts)
				}
			case diff.OpAddIndex:
				if idx, ok := op.After.(schema.SchemaIndex); ok {
					keysDoc := bson.D{}
					for _, col := range idx.Columns {
						keysDoc = append(keysDoc, bson.E{Key: col, Value: 1})
					}
					idxOpts := options.Index().SetName(idx.Name).SetUnique(idx.Unique)
					_, _ = db.Collection(collName).Indexes().CreateOne(ctx, mongo.IndexModel{
						Keys:    keysDoc,
						Options: idxOpts,
					})
				}
			case diff.OpDropIndex:
				_, _ = db.Collection(collName).Indexes().DropOne(ctx, op.ObjectName)
			case diff.OpDropTable:
				_ = db.Collection(collName).Drop(ctx)
			}
		}
	}

	return nil
}

// Create inserts or upserts a document into live MongoDB or in-memory fallback.
func (a *MongoAdapter) Create(ctx context.Context, ref model.ModelRef, data map[string]any) (map[string]any, error) {
	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}
	pkCol := ref.PrimaryKey
	if pkCol == "" {
		pkCol = "id"
	}

	doc := make(map[string]any)
	for k, v := range data {
		doc[k] = v
	}

	if _, ok := doc["_id"]; !ok {
		if idVal, hasID := doc[pkCol]; hasID && idVal != nil && fmt.Sprintf("%v", idVal) != "" {
			doc["_id"] = idVal
		} else if idVal, hasID := doc["id"]; hasID && idVal != nil && fmt.Sprintf("%v", idVal) != "" {
			doc["_id"] = idVal
			if pkCol != "id" {
				doc[pkCol] = idVal
			}
		} else {
			genID := mapping.GenerateUUID()
			doc["_id"] = genID
			doc[pkCol] = genID
			if pkCol != "id" {
				doc["id"] = genID
			}
		}
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	if client != nil {
		coll := client.Database(a.database).Collection(collName)
		filter := bson.M{
			"$or": []bson.M{
				{"_id": doc["_id"]},
				{pkCol: doc["_id"]},
				{"id": doc["_id"]},
			},
		}
		opts := options.Replace().SetUpsert(true)
		if _, err := coll.ReplaceOne(ctx, filter, doc, opts); err != nil {
			return nil, fmt.Errorf("failed inserting document into MongoDB '%s': %w", collName, err)
		}
		return doc, nil
	}

	// In-Memory Fallback
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mockStore[collName] = append(a.mockStore[collName], doc)
	return doc, nil
}

// Find queries documents in live MongoDB or in-memory fallback.
func (a *MongoAdapter) Find(ctx context.Context, ref model.ModelRef, q query.Query) ([]map[string]any, int64, error) {
	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	if client != nil {
		coll := client.Database(a.database).Collection(collName)
		filter := a.queryBuilder.BuildFilter(q)

		findOpts := options.Find()
		if q.Pagination.Offset > 0 {
			findOpts.SetSkip(int64(q.Pagination.Offset))
		}
		if q.Pagination.Limit > 0 {
			findOpts.SetLimit(int64(q.Pagination.Limit))
		}
		if len(q.Sorts) > 0 {
			sortMap := a.queryBuilder.BuildSort(q.Sorts)
			sortD := bson.D{}
			for k, v := range sortMap {
				sortD = append(sortD, bson.E{Key: k, Value: v})
			}
			findOpts.SetSort(sortD)
		}

		cursor, err := coll.Find(ctx, filter, findOpts)
		if err != nil {
			return nil, 0, fmt.Errorf("mongodb find failed: %w", err)
		}
		defer cursor.Close(ctx)

		var results []map[string]any
		for cursor.Next(ctx) {
			var doc map[string]any
			if err := cursor.Decode(&doc); err == nil {
				results = append(results, doc)
			}
		}
		total, _ := coll.CountDocuments(ctx, filter)
		return results, total, nil
	}

	// In-Memory Fallback
	a.mu.RLock()
	defer a.mu.RUnlock()

	docs := a.mockStore[collName]
	total := int64(len(docs))
	offset := q.Pagination.Offset
	limit := q.Pagination.Limit

	if offset > len(docs) {
		return []map[string]any{}, total, nil
	}

	end := len(docs)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	return docs[offset:end], total, nil
}

// FindOne retrieves a document by ID.
func (a *MongoAdapter) FindOne(ctx context.Context, ref model.ModelRef, id any) (map[string]any, error) {
	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}
	pkCol := ref.PrimaryKey
	if pkCol == "" {
		pkCol = "id"
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	if client != nil {
		coll := client.Database(a.database).Collection(collName)
		filter := bson.M{
			"$or": []bson.M{
				{"_id": id},
				{pkCol: id},
				{"id": id},
				{"_id": fmt.Sprintf("%v", id)},
				{pkCol: fmt.Sprintf("%v", id)},
				{"id": fmt.Sprintf("%v", id)},
			},
		}
		var doc map[string]any
		if err := coll.FindOne(ctx, filter).Decode(&doc); err != nil {
			return nil, fmt.Errorf("document '%v' not found: %w", id, err)
		}
		return doc, nil
	}

	// In-Memory Fallback
	a.mu.RLock()
	defer a.mu.RUnlock()

	idStr := fmt.Sprintf("%v", id)
	for _, doc := range a.mockStore[collName] {
		if fmt.Sprintf("%v", doc["_id"]) == idStr || fmt.Sprintf("%v", doc[pkCol]) == idStr || fmt.Sprintf("%v", doc["id"]) == idStr {
			return doc, nil
		}
	}
	return nil, fmt.Errorf("document '%v' not found", id)
}

// Update replaces a document by ID.
func (a *MongoAdapter) Update(ctx context.Context, ref model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}
	pkCol := ref.PrimaryKey
	if pkCol == "" {
		pkCol = "id"
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	if client != nil {
		coll := client.Database(a.database).Collection(collName)
		filter := bson.M{
			"$or": []bson.M{
				{"_id": id},
				{pkCol: id},
				{"id": id},
				{"_id": fmt.Sprintf("%v", id)},
				{pkCol: fmt.Sprintf("%v", id)},
				{"id": fmt.Sprintf("%v", id)},
			},
		}
		docCopy := make(map[string]any)
		for k, v := range data {
			docCopy[k] = v
		}
		if _, hasID := docCopy["_id"]; !hasID {
			docCopy["_id"] = id
		}
		if _, err := coll.ReplaceOne(ctx, filter, docCopy, options.Replace().SetUpsert(true)); err != nil {
			return nil, fmt.Errorf("mongodb update failed: %w", err)
		}
		return docCopy, nil
	}

	// In-Memory Fallback
	a.mu.Lock()
	defer a.mu.Unlock()

	idStr := fmt.Sprintf("%v", id)
	for i, doc := range a.mockStore[collName] {
		if fmt.Sprintf("%v", doc["_id"]) == idStr || fmt.Sprintf("%v", doc[pkCol]) == idStr || fmt.Sprintf("%v", doc["id"]) == idStr {
			docCopy := make(map[string]any)
			for k, v := range data {
				docCopy[k] = v
			}
			docCopy["_id"] = doc["_id"]
			a.mockStore[collName][i] = docCopy
			return docCopy, nil
		}
	}
	return nil, fmt.Errorf("document '%v' not found", id)
}

// Patch updates specific fields by ID.
func (a *MongoAdapter) Patch(ctx context.Context, ref model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}
	pkCol := ref.PrimaryKey
	if pkCol == "" {
		pkCol = "id"
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return nil, err
	}
	if client != nil {
		coll := client.Database(a.database).Collection(collName)
		filter := bson.M{
			"$or": []bson.M{
				{"_id": id},
				{pkCol: id},
				{"id": id},
				{"_id": fmt.Sprintf("%v", id)},
				{pkCol: fmt.Sprintf("%v", id)},
				{"id": fmt.Sprintf("%v", id)},
			},
		}
		update := bson.M{"$set": data}
		opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
		var updatedDoc map[string]any
		if err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&updatedDoc); err != nil {
			return nil, fmt.Errorf("mongodb patch failed: %w", err)
		}
		return updatedDoc, nil
	}

	// In-Memory Fallback
	a.mu.Lock()
	defer a.mu.Unlock()

	idStr := fmt.Sprintf("%v", id)
	for i, doc := range a.mockStore[collName] {
		if fmt.Sprintf("%v", doc["_id"]) == idStr || fmt.Sprintf("%v", doc[pkCol]) == idStr || fmt.Sprintf("%v", doc["id"]) == idStr {
			for k, v := range data {
				a.mockStore[collName][i][k] = v
			}
			return a.mockStore[collName][i], nil
		}
	}
	return nil, fmt.Errorf("document '%v' not found", id)
}

// Delete removes a document by ID.
func (a *MongoAdapter) Delete(ctx context.Context, ref model.ModelRef, id any) error {
	collName := ref.StorageName
	if collName == "" {
		collName = ref.Name
	}
	pkCol := ref.PrimaryKey
	if pkCol == "" {
		pkCol = "id"
	}

	client, err := a.getClient(ctx)
	if err != nil {
		return err
	}
	if client != nil {
		coll := client.Database(a.database).Collection(collName)
		filter := bson.M{
			"$or": []bson.M{
				{"_id": id},
				{pkCol: id},
				{"id": id},
				{"_id": fmt.Sprintf("%v", id)},
				{pkCol: fmt.Sprintf("%v", id)},
				{"id": fmt.Sprintf("%v", id)},
			},
		}
		res, err := coll.DeleteOne(ctx, filter)
		if err != nil {
			return fmt.Errorf("mongodb delete failed: %w", err)
		}
		if res.DeletedCount == 0 {
			return fmt.Errorf("document '%v' not found", id)
		}
		return nil
	}

	// In-Memory Fallback
	a.mu.Lock()
	defer a.mu.Unlock()

	idStr := fmt.Sprintf("%v", id)
	for i, doc := range a.mockStore[collName] {
		if fmt.Sprintf("%v", doc["_id"]) == idStr || fmt.Sprintf("%v", doc[pkCol]) == idStr || fmt.Sprintf("%v", doc["id"]) == idStr {
			a.mockStore[collName] = append(a.mockStore[collName][:i], a.mockStore[collName][i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("document '%v' not found", id)
}

// Execute runs native MongoDB commands or database operations.
func (a *MongoAdapter) Execute(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error) {
	switch req.Operation {
	case operation.OpCommand, operation.OpCustom:
		client, err := a.getClient(ctx)
		if err == nil && client != nil {
			db := client.Database(a.database)
			var cmdDoc bson.D
			if req.Target == "ping" {
				cmdDoc = bson.D{{Key: "ping", Value: 1}}
			} else {
				cmdDoc = bson.D{{Key: req.Target, Value: req.Arguments}}
			}
			var result bson.M
			if err := db.RunCommand(ctx, cmdDoc).Decode(&result); err == nil {
				return &execution.ExecutionResult{
					Data:   result,
					Status: "SUCCESS",
				}, nil
			}
		}
		return &execution.ExecutionResult{
			Data:   map[string]any{"command": req.Target, "arguments": req.Arguments},
			Status: "SUCCESS",
		}, nil

	case operation.OpFunction, operation.OpProcedure:
		return nil, adapter.ErrOperationNotSupported

	default:
		return nil, adapter.ErrOperationNotSupported
	}
}

func (a *MongoAdapter) Begin(ctx context.Context) (adapter.Transaction, error) {
	client, err := a.getClient(ctx)
	if err == nil && client != nil {
		session, err := client.StartSession()
		if err == nil {
			_ = session.StartTransaction()
			return &MongoTransaction{adapter: a, session: session}, nil
		}
	}
	return &MongoTransaction{adapter: a}, nil
}

// MongoTransaction implements adapter.Transaction for live or in-memory MongoDB transactions.
type MongoTransaction struct {
	adapter *MongoAdapter
	session mongo.Session
}

func (t *MongoTransaction) Create(ctx context.Context, m model.ModelRef, data map[string]any) (map[string]any, error) {
	if t.session != nil {
		var res map[string]any
		err := mongo.WithSession(ctx, t.session, func(sc mongo.SessionContext) error {
			var createErr error
			res, createErr = t.adapter.Create(sc, m, data)
			return createErr
		})
		return res, err
	}
	return t.adapter.Create(ctx, m, data)
}

func (t *MongoTransaction) Find(ctx context.Context, m model.ModelRef, q query.Query) ([]map[string]any, int64, error) {
	if t.session != nil {
		var res []map[string]any
		var total int64
		err := mongo.WithSession(ctx, t.session, func(sc mongo.SessionContext) error {
			var findErr error
			res, total, findErr = t.adapter.Find(sc, m, q)
			return findErr
		})
		return res, total, err
	}
	return t.adapter.Find(ctx, m, q)
}

func (t *MongoTransaction) FindOne(ctx context.Context, m model.ModelRef, id any) (map[string]any, error) {
	if t.session != nil {
		var res map[string]any
		err := mongo.WithSession(ctx, t.session, func(sc mongo.SessionContext) error {
			var findErr error
			res, findErr = t.adapter.FindOne(sc, m, id)
			return findErr
		})
		return res, err
	}
	return t.adapter.FindOne(ctx, m, id)
}

func (t *MongoTransaction) Update(ctx context.Context, m model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	if t.session != nil {
		var res map[string]any
		err := mongo.WithSession(ctx, t.session, func(sc mongo.SessionContext) error {
			var updateErr error
			res, updateErr = t.adapter.Update(sc, m, id, data)
			return updateErr
		})
		return res, err
	}
	return t.adapter.Update(ctx, m, id, data)
}

func (t *MongoTransaction) Patch(ctx context.Context, m model.ModelRef, id any, data map[string]any) (map[string]any, error) {
	if t.session != nil {
		var res map[string]any
		err := mongo.WithSession(ctx, t.session, func(sc mongo.SessionContext) error {
			var patchErr error
			res, patchErr = t.adapter.Patch(sc, m, id, data)
			return patchErr
		})
		return res, err
	}
	return t.adapter.Patch(ctx, m, id, data)
}

func (t *MongoTransaction) Delete(ctx context.Context, m model.ModelRef, id any) error {
	if t.session != nil {
		return mongo.WithSession(ctx, t.session, func(sc mongo.SessionContext) error {
			return t.adapter.Delete(sc, m, id)
		})
	}
	return t.adapter.Delete(ctx, m, id)
}

func (t *MongoTransaction) Execute(ctx context.Context, req execution.ExecutionRequest) (*execution.ExecutionResult, error) {
	return t.adapter.Execute(ctx, req)
}

func (t *MongoTransaction) Commit(ctx context.Context) error {
	if t.session != nil {
		defer t.session.EndSession(ctx)
		return t.session.CommitTransaction(ctx)
	}
	return nil
}

func (t *MongoTransaction) Rollback(ctx context.Context) error {
	if t.session != nil {
		defer t.session.EndSession(ctx)
		return t.session.AbortTransaction(ctx)
	}
	return nil
}
