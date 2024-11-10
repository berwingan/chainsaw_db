package types

import (
	"testing"
	"time"
)

func TestColumnValidate(t *testing.T) {
	tests := []struct {
		name    string
		column  Column
		value   interface{}
		wantErr bool
	}{
		{
			name:    "valid string",
			column:  Column{Name: "name", Type: TypeString},
			value:   "test",
			wantErr: false,
		},
		{
			name:    "invalid string",
			column:  Column{Name: "name", Type: TypeString},
			value:   123,
			wantErr: true,
		},
		{
			name:    "valid int",
			column:  Column{Name: "age", Type: TypeInt},
			value:   42,
			wantErr: false,
		},
		{
			name:    "invalid int",
			column:  Column{Name: "age", Type: TypeInt},
			value:   "42",
			wantErr: true,
		},
		{
			name:    "valid float",
			column:  Column{Name: "price", Type: TypeFloat},
			value:   42.5,
			wantErr: false,
		},
		{
			name:    "valid bool",
			column:  Column{Name: "active", Type: TypeBool},
			value:   true,
			wantErr: false,
		},
		{
			name:    "valid timestamp",
			column:  Column{Name: "created", Type: TypeTimestamp},
			value:   time.Now(),
			wantErr: false,
		},
		{
			name:    "null with not null constraint",
			column:  Column{Name: "required", Type: TypeString, NotNull: true},
			value:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.column.Validate(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Column.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaValidate(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: TypeString, NotNull: true},
			{Name: "age", Type: TypeInt},
			{Name: "created", Type: TypeTimestamp, NotNull: true},
		},
		PrimaryKey: "id",
	}

	tests := []struct {
		name    string
		row     Row
		wantErr bool
	}{
		{
			name: "valid row",
			row: Row{
				"id":      1,
				"name":    "John",
				"age":     30,
				"created": time.Now(),
			},
			wantErr: false,
		},
		{
			name: "missing required field",
			row: Row{
				"id":   1,
				"name": "John",
			},
			wantErr: true,
		},
		{
			name: "invalid type",
			row: Row{
				"id":      "1", // should be int
				"name":    "John",
				"created": time.Now(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := schema.Validate(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("Schema.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
