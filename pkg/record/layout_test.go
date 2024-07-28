package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordLayout(t *testing.T) {
	schema := NewSchema()
	schema.AddField("id", SCHEMA_TYPE_INTEGER, 0)
	schema.AddField("name", SCHEMA_TYPE_VARCHAR, 20)
	layout, err := NewLayoutFromSchema(schema)
	assert.NoError(t, err)

	assert.Equal(t, 4, layout.Offset("id"))
	assert.Equal(t, 8, layout.Offset("name"))
	assert.Equal(t, 4+4+(4+4*20), layout.SlotSize())
}
