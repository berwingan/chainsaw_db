package engine

import (
	"fmt"
	"sync"
	"time"

	"chainsaw_db/internal/storage"
	"chainsaw_db/internal/types"
)

// Engine represents the database engine
type Engine struct {
	mu           sync.RWMutex
	store        *storage.KVStore
	tables       map[string]*storage.Table
	transactions map[string]*Transaction
	config       types.Config
	stats        types.Statistics
}

// New creates a new database engine instance
func New(config types.Config) (*Engine, error) {
	// Initialize KV store
	kvConfig := storage.KVConfig{
		DataDir:     config.DataDir,
		EnableWAL:   config.EnableWAL,
		CompactSize: int64(config.PageSize * 1024), // Convert to bytes
	}

	store, err := storage.NewKVStore(kvConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	engine := &Engine{
		store:        store,
		tables:       make(map[string]*storage.Table),
		transactions: make(map[string]*Transaction),
		config:       config,
		stats: types.Statistics{
			StartTime: time.Now(),
		},
	}

	// Load existing tables
	if err := engine.loadTables(); err != nil {
		return nil, fmt.Errorf("failed to load tables: %w", err)
	}

	return engine, nil
}

// CreateTable creates a new table with the given name and schema
func (e *Engine) CreateTable(name string, schema types.Schema) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if table already exists
	if _, exists := e.tables[name]; exists {
		return &types.DBError{
			Type:    types.ErrTypeValidation,
			Message: fmt.Sprintf("table %s already exists", name),
		}
	}

	// Create new table
	table, err := storage.NewTable(name, schema, e.store)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	e.tables[name] = table
	e.stats.TotalQueries++
	return nil
}

// DropTable removes a table from the database
func (e *Engine) DropTable(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	table, exists := e.tables[name]
	if !exists {
		return &types.DBError{
			Type:    types.ErrTypeNotFound,
			Message: fmt.Sprintf("table %s not found", name),
		}
	}

	// Delete all table data
	iter, err := table.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}
	defer iter.Release()

	for iter.Next() {
		row, err := iter.Row()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
		if err := table.Delete(row[table.Schema().PrimaryKey]); err != nil {
			return fmt.Errorf("failed to delete row: %w", err)
		}
	}

	// Remove table metadata
	delete(e.tables, name)
	e.stats.TotalQueries++
	return nil
}

// Execute executes a query on the database
func (e *Engine) Execute(query types.Query) (*types.Result, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table, exists := e.tables[query.TableName]
	if !exists {
		return nil, &types.DBError{
			Type:    types.ErrTypeNotFound,
			Message: fmt.Sprintf("table %s not found", query.TableName),
		}
	}

	result := &types.Result{}

	switch query.Type {
	case types.SelectQuery:
		rows, err := e.executeSelect(table, query)
		if err != nil {
			return nil, err
		}
		result.Rows = rows

	case types.InsertQuery:
		if len(query.Columns) != len(query.Values) {
			return nil, &types.DBError{
				Type:    types.ErrTypeValidation,
				Message: "column and value count mismatch",
			}
		}

		row := make(types.Row)
		for i, col := range query.Columns {
			row[col] = query.Values[i]
		}

		if err := table.Insert(row); err != nil {
			return nil, err
		}
		result.RowsAffected = 1

	case types.UpdateQuery:
		affected, err := e.executeUpdate(table, query)
		if err != nil {
			return nil, err
		}
		result.RowsAffected = affected

	case types.DeleteQuery:
		affected, err := e.executeDelete(table, query)
		if err != nil {
			return nil, err
		}
		result.RowsAffected = affected

	default:
		return nil, &types.DBError{
			Type:    types.ErrTypeSyntax,
			Message: "unsupported query type",
		}
	}

	e.stats.TotalQueries++
	return result, nil
}

// BeginTransaction starts a new transaction
func (e *Engine) BeginTransaction() (*Transaction, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx := NewTransaction()
	e.transactions[tx.ID] = tx
	e.stats.TotalTx++
	e.stats.ActiveTx++

	return tx, nil
}

// GetStats returns current database statistics
func (e *Engine) GetStats() types.Statistics {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// Internal helper methods

func (e *Engine) loadTables() error {
	// Implementation would scan the KV store for table metadata
	// and reconstruct table objects
	return nil
}

func (e *Engine) executeSelect(table *storage.Table, query types.Query) ([]types.Row, error) {
	// Basic implementation - should be enhanced with proper condition evaluation
	var rows []types.Row
	iter, err := table.Scan()
	if err != nil {
		return nil, err
	}
	defer iter.Release()

	for iter.Next() {
		row, err := iter.Row()
		if err != nil {
			return nil, err
		}

		if matchesConditions(row, query.Conditions) {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

func (e *Engine) executeUpdate(table *storage.Table, query types.Query) (int64, error) {
	var affected int64
	iter, err := table.Scan()
	if err != nil {
		return 0, err
	}
	defer iter.Release()

	updates := make(types.Row)
	for i, col := range query.Columns {
		updates[col] = query.Values[i]
	}

	for iter.Next() {
		row, err := iter.Row()
		if err != nil {
			return affected, err
		}

		if matchesConditions(row, query.Conditions) {
			if err := table.Update(row[table.Schema().PrimaryKey], updates); err != nil {
				return affected, err
			}
			affected++
		}
	}

	return affected, nil
}

func (e *Engine) executeDelete(table *storage.Table, query types.Query) (int64, error) {
	var affected int64
	iter, err := table.Scan()
	if err != nil {
		return 0, err
	}
	defer iter.Release()

	for iter.Next() {
		row, err := iter.Row()
		if err != nil {
			return affected, err
		}

		if matchesConditions(row, query.Conditions) {
			if err := table.Delete(row[table.Schema().PrimaryKey]); err != nil {
				return affected, err
			}
			affected++
		}
	}

	return affected, nil
}

func matchesConditions(row types.Row, conditions []types.Condition) bool {
	for _, cond := range conditions {
		value := row[cond.Column]
		if !evaluateCondition(value, cond.Operator, cond.Value) {
			return false
		}
	}
	return true
}

func evaluateCondition(value interface{}, operator string, target interface{}) bool {
	// Basic implementation - should be enhanced with type-aware comparisons
	switch operator {
	case "=":
		return value == target
	case ">":
		return compareValues(value, target) > 0
	case "<":
		return compareValues(value, target) < 0
	case ">=":
		return compareValues(value, target) >= 0
	case "<=":
		return compareValues(value, target) <= 0
	case "!=":
		return value != target
	default:
		return false
	}
}

func compareValues(a, b interface{}) int {
	// Basic implementation - should be enhanced with proper type handling
	switch v1 := a.(type) {
	case int:
		if v2, ok := b.(int); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case string:
		if v2, ok := b.(string); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	}
	return 0
}
