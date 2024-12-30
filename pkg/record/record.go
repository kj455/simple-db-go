package record

import "github.com/kj455/simple-db/pkg/file"

type SchemaType int

const (
	SCHEMA_TYPE_INTEGER SchemaType = 1
	SCHEMA_TYPE_VARCHAR SchemaType = 2
)

// Schema holds a record's schema
type Schema interface {
	AddField(field string, typ SchemaType, length int)
	AddIntField(field string)
	AddStringField(field string, length int)
	Add(field string, sch Schema) error
	AddAll(sch Schema) error
	Fields() []string
	HasField(field string) bool
	Type(field string) (SchemaType, error)
	Length(field string) (int, error)
}

type Layout interface {
	Schema() Schema
	Offset(field string) int
	SlotSize() int
}

type RecordPage interface {
	GetInt(slot int, field string) (int, error)
	GetString(slot int, field string) (string, error)
	SetInt(slot int, field string, val int) error
	SetString(slot int, field string, val string) error
	// Format initializes the record page
	Format() error
	Delete(slot int) error
	// NextAfter returns the next slot after the given slot
	NextAfter(slot int) int
	// InsertAfter inserts a new record after the given slot
	InsertAfter(slot int) (int, error)
	Block() file.BlockId
}

// RID is a record identifier holding a block number and slot number
type RID interface {
	BlockNumber() int
	Slot() int
	Equals(other RID) bool
	String() string
}

type TableScan interface {
	GetInt(field string) (int, error)
	GetString(field string) (string, error)
	GetVal(field string) (any, error)
	SetInt(field string, val int) error
	SetString(field string, val string) error
	SetVal(field string, val any) error
	HasField(field string) bool
	BeforeFirst() error
	Next() bool
	Close()
	Insert() error
	Delete() error
	MoveToRID(rid RID) error
	GetRID() RID
}
