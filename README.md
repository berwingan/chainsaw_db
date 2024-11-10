# chainsaw_db
Database implementation in Go




chainsaw_db/
├── cmd/
│   └── chainsaw_db/
│       └── main.go           # Main application entry point
├── internal/
│   ├── engine/
│   │   ├── engine.go        # Database engine core
│   │   ├── transaction.go   # Transaction management
│   │   └── errors.go        # Custom error definitions
│   ├── storage/
│   │   ├── kv.go           # Key-value store implementation
│   │   ├── table.go        # Table operations
│   │   └── index.go        # Indexing implementations
│   ├── query/
│   │   ├── parser.go       # Query parsing
│   │   ├── executor.go     # Query execution
│   │   └── builder.go      # Query builder
│   └── types/
│       └── types.go        # Shared types and interfaces
├── pkg/
│   └── utils/
│       ├── logger.go       # Logging utilities
│       └── serializer.go   # Data serialization utilities
├── examples/
│   └── basic_usage.go      # Usage examples
├── go.mod
└── README.md



1) Types
2) KV
3) Tables
4) Engine
5) Index
6) Transaction
7) Parser
8) Executor
9) Builder
10) Logger
11) Serializer
12) Main