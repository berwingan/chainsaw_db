package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"chainsaw_db/internal/types"
)

// Table represents a database table
type Table struct {
	mu       sync.RWMutex
	name     string
	schema   types.Schema
	store    *KVStore
	metadata types.TableMetadata
}

// NewTable creates a new table with the given name and schema
func NewTable(name string, schema types.Schema, store *KVStore) (*Table, error) {
	if store == nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeValidation,
			Message: "store cannot be nil",
		}
	}

	metadata := types.TableMetadata{
		Name:      name,
		Schema:    schema,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		RowCount:  0,
	}

	// Store the metadata
	metadataKey := []byte(fmt.Sprintf("table:%s:metadata", name))
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to marshal table metadata",
			Err:     err,
		}
	}

	if err := store.Put(metadataKey, metadataBytes); err != nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to store table metadata",
			Err:     err,
		}
	}

	return &Table{
		name:     name,
		schema:   schema,
		store:    store,
		metadata: metadata,
	}, nil
}

// Insert adds a new row to the table
func (t *Table) Insert(row types.Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Validate row against schema
	if err := t.schema.Validate(row); err != nil {
		return &types.DBError{
			Type:    types.ErrTypeValidation,
			Message: "row validation failed",
			Err:     err,
		}
	}

	// Check primary key constraint
	if t.schema.PrimaryKey != "" {
		pkValue := row[t.schema.PrimaryKey]
		exists, err := t.hasKey(pkValue)
		if err != nil {
			return err
		}
		if exists {
			return &types.DBError{
				Type:    types.ErrTypeConstraint,
				Message: fmt.Sprintf("duplicate primary key: %v", pkValue),
			}
		}
	}

	// Generate row key
	rowKey := t.generateRowKey(row)

	// Serialize row data
	rowBytes, err := json.Marshal(row)
	if err != nil {
		return &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to marshal row data",
			Err:     err,
		}
	}

	// Store the row
	if err := t.store.Put(rowKey, rowBytes); err != nil {
		return &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to store row",
			Err:     err,
		}
	}

	// Update row count
	t.metadata.RowCount++
	t.metadata.UpdatedAt = time.Now()
	return t.updateMetadata()
}

// Get retrieves a row by its primary key
func (t *Table) Get(pkValue interface{}) (types.Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.schema.PrimaryKey == "" {
		return nil, &types.DBError{
			Type:    types.ErrTypeValidation,
			Message: "table has no primary key",
		}
	}

	rowKey := t.generateKeyFromPK(pkValue)
	rowBytes, err := t.store.Get(rowKey)
	if err != nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to retrieve row",
			Err:     err,
		}
	}

	if rowBytes == nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeNotFound,
			Message: fmt.Sprintf("row with primary key %v not found", pkValue),
		}
	}

	var row types.Row
	if err := json.Unmarshal(rowBytes, &row); err != nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to unmarshal row data",
			Err:     err,
		}
	}

	return row, nil
}

// Update modifies an existing row
func (t *Table) Update(pkValue interface{}, updates types.Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Get existing row
	existing, err := t.Get(pkValue)
	if err != nil {
		return err
	}

	// Apply updates
	for k, v := range updates {
		existing[k] = v
	}

	// Validate updated row
	if err := t.schema.Validate(existing); err != nil {
		return &types.DBError{
			Type:    types.ErrTypeValidation,
			Message: "updated row validation failed",
			Err:     err,
		}
	}

	// Store updated row
	rowKey := t.generateKeyFromPK(pkValue)
	rowBytes, err := json.Marshal(existing)
	if err != nil {
		return &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to marshal updated row",
			Err:     err,
		}
	}

	if err := t.store.Put(rowKey, rowBytes); err != nil {
		return &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to store updated row",
			Err:     err,
		}
	}

	t.metadata.UpdatedAt = time.Now()
	return t.updateMetadata()
}

// Delete removes a row by its primary key
func (t *Table) Delete(pkValue interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if row exists
	if _, err := t.Get(pkValue); err != nil {
		return err
	}

	rowKey := t.generateKeyFromPK(pkValue)
	if err := t.store.Delete(rowKey); err != nil {
		return &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to delete row",
			Err:     err,
		}
	}

	t.metadata.RowCount--
	t.metadata.UpdatedAt = time.Now()
	return t.updateMetadata()
}

// Scan returns an iterator over all rows in the table
func (t *Table) Scan() (*TableIterator, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	iter := t.store.NewIterator()
	return &TableIterator{
		table:    t,
		iterator: iter,
	}, nil
}

// Helper methods

func (t *Table) hasKey(pkValue interface{}) (bool, error) {
	rowKey := t.generateKeyFromPK(pkValue)
	value, err := t.store.Get(rowKey)
	if err != nil {
		return false, &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to check key existence",
			Err:     err,
		}
	}
	return value != nil, nil
}

func (t *Table) generateRowKey(row types.Row) []byte {
	pkValue := row[t.schema.PrimaryKey]
	return t.generateKeyFromPK(pkValue)
}

func (t *Table) generateKeyFromPK(pkValue interface{}) []byte {
	return []byte(fmt.Sprintf("table:%s:row:%v", t.name, pkValue))
}

func (t *Table) updateMetadata() error {
	metadataKey := []byte(fmt.Sprintf("table:%s:metadata", t.name))
	metadataBytes, err := json.Marshal(t.metadata)
	if err != nil {
		return &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to marshal updated metadata",
			Err:     err,
		}
	}

	return t.store.Put(metadataKey, metadataBytes)
}

// TableIterator provides iteration over table rows
type TableIterator struct {
	table    *Table
	iterator *Iterator
}

func (it *TableIterator) Next() bool {
	return it.iterator.Next()
}

func (it *TableIterator) Row() (types.Row, error) {
	valueBytes := it.iterator.Value()
	if valueBytes == nil {
		return nil, nil
	}

	var row types.Row
	if err := json.Unmarshal(valueBytes, &row); err != nil {
		return nil, &types.DBError{
			Type:    types.ErrTypeIO,
			Message: "failed to unmarshal row data",
			Err:     err,
		}
	}

	return row, nil
}

func (it *TableIterator) Release() {
	it.iterator.Release()
}
