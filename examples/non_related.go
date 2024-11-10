package main

import (
	"fmt"
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

func main() {
	// Test with different DataType values
	var dataTypes = []DataType{
		TypeNull,
		TypeString,
		TypeInt,
		TypeFloat,
		TypeBool,
		TypeTimestamp,
		TypeBlob,
	}

	// Iterate through the DataType values and print their string representations
	for _, dt := range dataTypes {
		fmt.Printf("DataType: %v, String: %s\n", dt, dt.String())
	}
}
