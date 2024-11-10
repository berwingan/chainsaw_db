package types

import (
	"fmt"
	"time"
)

// DataType represents the supported data types in the database
type DataType int

const (
	TypeNull DataType = iota
	TypeString
	TypeInt
	TypeFloat
	TypeBool
	TypeTimestamp
	TypeBlob
)

// String provides string representation of DataType
func (dt DataType) String() string {
	switch dt {
	case TypeNull:
		return "NULL"
	case TypeString:
		return "STRING"
	case TypeInt:
		return "INTEGER"
	case TypeFloat:
		return "FLOAT"
	case TypeBool:
		return "BOOLEAN"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeBlob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}

// Column represents a database column definition
type Column struct {
	Name       string
	Type       DataType
	PrimaryKey bool
	NotNull    bool
	Unique     bool
	Default    interface{}
}

// Validate checks if a value matches the column's type constraints
func (c Column) Validate(value interface{}) error {
	if value == nil {
		if c.NotNull {
			return fmt.Errorf("column %s cannot be null", c.Name)
		}
		return nil
	}

	switch c.Type {
	case TypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("column %s expects string, got %T", c.Name, value)
		}
	case TypeInt:
		switch v := value.(type) {
		case int, int32, int64:
			// Valid
		default:
			return fmt.Errorf("column %s expects integer, got %T", c.Name, v)
		}
	case TypeFloat:
		switch v := value.(type) {
		case float32, float64:
			// Valid
		default:
			return fmt.Errorf("column %s expects float, got %T", c.Name, v)
		}
	case TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("column %s expects boolean, got %T", c.Name, value)
		}
	case TypeTimestamp:
		if _, ok := value.(time.Time); !ok {
			return fmt.Errorf("column %s expects timestamp, got %T", c.Name, value)
		}
	}

	return nil
}

// Row represents a single row in a table
type Row map[string]interface{}

// Schema represents the structure of a table
type Schema struct {
	Columns     []Column
	PrimaryKey  string
	UniqueKeys  []string
	IndexedCols []string
}

// Validate checks if a row conforms to the schema
func (s Schema) Validate(row Row) error {
	// Check all required columns are present
	for _, col := range s.Columns {
		value, exists := row[col.Name]
		if !exists {
			if col.NotNull && col.Default == nil {
				return fmt.Errorf("missing required column: %s", col.Name)
			}
			continue
		}
		if err := col.Validate(value); err != nil {
			return err
		}
	}
	return nil
}

// TableMetadata stores metadata about a table
type TableMetadata struct {
	Name      string
	Schema    Schema
	CreatedAt time.Time
	UpdatedAt time.Time
	RowCount  int64
}

// IndexType represents different types of indexes
type IndexType int

const (
	BTreeIndex IndexType = iota
	HashIndex
)

// IndexMetadata stores information about an index
type IndexMetadata struct {
	Name      string
	TableName string
	Columns   []string
	Type      IndexType
	Unique    bool
	CreatedAt time.Time
}

// QueryType represents different types of queries
type QueryType int

const (
	SelectQuery QueryType = iota
	InsertQuery
	UpdateQuery
	DeleteQuery
	CreateTableQuery
	DropTableQuery
	CreateIndexQuery
)

// Query represents a database query
type Query struct {
	Type       QueryType
	TableName  string
	Columns    []string
	Values     []interface{}
	Conditions []Condition
	Joins      []Join
	OrderBy    []OrderByClause
	Limit      int
	Offset     int
}

// Condition represents a WHERE clause condition
type Condition struct {
	Column   string
	Operator string
	Value    interface{}
	LogicOp  string // AND, OR
}

// Join represents a table join
type Join struct {
	Type     string // INNER, LEFT, RIGHT
	Table    string
	OnClause Condition
}

// OrderByClause represents ORDER BY specifications
type OrderByClause struct {
	Column string
	Desc   bool
}

// TransactionState represents the state of a transaction
type TransactionState int

const (
	TxPending TransactionState = iota
	TxCommitted
	TxRolledBack
)

// Transaction represents a database transaction
type Transaction struct {
	ID        string
	State     TransactionState
	StartTime time.Time
	EndTime   time.Time
}

// Result represents the result of a query execution
type Result struct {
	RowsAffected int64
	LastInsertID int64
	Rows         []Row
	Error        error
}

// ErrorType represents different types of database errors
type ErrorType int

const (
	ErrTypeValidation ErrorType = iota
	ErrTypeConstraint
	ErrTypeNotFound
	ErrTypeIO
	ErrTypeTransaction
	ErrTypeSyntax
)

// DBError represents a database error
type DBError struct {
	Type    ErrorType
	Message string
	Err     error
}

func (e DBError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Config represents database configuration
type Config struct {
	DataDir        string
	MaxConnections int
	BufferSize     int64
	LogLevel       string
	EnableWAL      bool
	PageSize       int
}

// Statistics represents database statistics
type Statistics struct {
	StartTime    time.Time
	TotalQueries int64
	TotalTx      int64
	ActiveTx     int
	DiskUsage    int64
	CacheHits    int64
	CacheMisses  int64
}
