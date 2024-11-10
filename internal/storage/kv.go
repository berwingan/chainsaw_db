package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry represents a single key-value pair with metadata
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Deleted   bool
}

// KVStore represents a persistent key-value store
type KVStore struct {
	mu           sync.RWMutex
	data         map[string]Entry
	filepath     string
	indexFile    string
	walFile      string
	isCompacting bool
	compactMu    sync.Mutex
}

// KVConfig holds configuration for the KV store
type KVConfig struct {
	DataDir     string
	EnableWAL   bool
	CompactSize int64 // Size in bytes that triggers compaction
}

// KVError represents errors specific to the key-value store
type KVError struct {
	Op  string // Operation that failed
	Err error  // Underlying error
}

func (e *KVError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("kv error during %s: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("kv error during %s", e.Op)
}

// NewKVStore creates a new key-value store
func NewKVStore(config KVConfig) (*KVStore, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, &KVError{Op: "init", Err: err}
	}

	store := &KVStore{
		data:      make(map[string]Entry),
		filepath:  filepath.Join(config.DataDir, "store.db"),
		indexFile: filepath.Join(config.DataDir, "store.index"),
		walFile:   filepath.Join(config.DataDir, "wal.log"),
	}

	// Load existing data
	if err := store.load(); err != nil {
		return nil, &KVError{Op: "load", Err: err}
	}

	return store, nil
}

// Get retrieves a value by key
func (s *KVStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[string(key)]
	if !exists || entry.Deleted {
		return nil, nil
	}

	return entry.Value, nil
}

// Put stores a key-value pair
func (s *KVStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Deleted:   false,
	}

	// Write to WAL first
	if err := s.writeWAL(entry); err != nil {
		return &KVError{Op: "put", Err: err}
	}

	// Update in-memory
	s.data[string(key)] = entry

	return s.persist()
}

// Delete removes a key-value pair
func (s *KVStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := Entry{
		Key:       key,
		Timestamp: time.Now().UnixNano(),
		Deleted:   true,
	}

	// Write to WAL first
	if err := s.writeWAL(entry); err != nil {
		return &KVError{Op: "delete", Err: err}
	}

	// Mark as deleted in-memory
	s.data[string(key)] = entry

	return s.persist()
}

// persist writes the current state to disk
func (s *KVStore) persist() error {
	tempFile := s.filepath + ".tmp"

	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(s.data); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to encode data: %w", err)
	}

	// Ensure all data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, s.filepath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// load reads the store from disk
func (s *KVStore) load() error {
	file, err := os.Open(s.filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // New database
		}
		return fmt.Errorf("failed to open store file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&s.data); err != nil {
		return fmt.Errorf("failed to decode store data: %w", err)
	}

	// Replay WAL if it exists
	return s.replayWAL()
}

// writeWAL writes an entry to the WAL file
func (s *KVStore) writeWAL(entry Entry) error {
	file, err := os.OpenFile(s.walFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	// Write entry to WAL
	if err := json.NewEncoder(file).Encode(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return file.Sync()
}

// replayWAL replays the write-ahead log
func (s *KVStore) replayWAL() error {
	file, err := os.Open(s.walFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var entry Entry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode WAL entry: %w", err)
		}

		s.data[string(entry.Key)] = entry
	}

	// Clear WAL after successful replay
	return os.Truncate(s.walFile, 0)
}

// Compact removes deleted entries and old versions
func (s *KVStore) Compact() error {
	s.compactMu.Lock()
	if s.isCompacting {
		s.compactMu.Unlock()
		return nil
	}
	s.isCompacting = true
	s.compactMu.Unlock()

	defer func() {
		s.compactMu.Lock()
		s.isCompacting = false
		s.compactMu.Unlock()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove deleted entries
	compacted := make(map[string]Entry)
	for k, v := range s.data {
		if !v.Deleted {
			compacted[k] = v
		}
	}

	s.data = compacted
	return s.persist()
}

// Iterator returns an iterator over the key-value pairs
type Iterator struct {
	store    *KVStore
	keys     []string
	current  int
	released bool
	mu       sync.Mutex
}

// NewIterator creates a new iterator over the store
func (s *KVStore) NewIterator() *Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k, v := range s.data {
		if !v.Deleted {
			keys = append(keys, k)
		}
	}

	return &Iterator{
		store:    s,
		keys:     keys,
		current:  0,
		released: false,
	}
}

// Next moves the iterator to the next key-value pair
func (it *Iterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.released || it.current >= len(it.keys) {
		return false
	}

	it.current++
	return it.current < len(it.keys)
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.released || it.current >= len(it.keys) {
		return nil
	}

	return []byte(it.keys[it.current])
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.released || it.current >= len(it.keys) {
		return nil
	}

	value, _ := it.store.Get([]byte(it.keys[it.current]))
	return value
}

// Release releases the iterator resources
func (it *Iterator) Release() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.released = true
	it.keys = nil
}
