package record

import (
	"fmt"

	"github.com/kj455/db/pkg/file"
)

type LayoutImpl struct {
	schema   Schema
	offsets  map[string]int
	slotSize int
}

func NewLayoutFromSchema(schema Schema) (Layout, error) {
	l := &LayoutImpl{
		schema:  schema,
		offsets: make(map[string]int),
	}
	pos := 4 // for int32 slot
	for _, field := range schema.Fields() {
		l.offsets[field] = pos
		length, err := l.lengthInBytes(field)
		if err != nil {
			return nil, fmt.Errorf("record: layout: length in bytes: %w", err)
		}
		pos += length
	}
	l.slotSize = pos
	return l, nil
}

func NewLayout(schema Schema, offsets map[string]int, slotSize int) Layout {
	return &LayoutImpl{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *LayoutImpl) Schema() Schema {
	return l.schema
}

func (l *LayoutImpl) Offset(field string) int {
	return l.offsets[field]
}

func (l *LayoutImpl) SlotSize() int {
	return l.slotSize
}

func (l *LayoutImpl) lengthInBytes(field string) (int, error) {
	typ, err := l.schema.Type(field)
	if err != nil {
		return 0, err
	}
	switch typ {
	case SCHEMA_TYPE_INTEGER:
		return 4, nil // for int32
	case SCHEMA_TYPE_VARCHAR:
		len, err := l.schema.Length(field)
		if err != nil {
			return 0, err
		}
		return file.MaxLength(len), nil
	}
	return 0, fmt.Errorf("record: layout: length in bytes: unknown type %v", typ)
}
