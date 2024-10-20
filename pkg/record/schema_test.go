package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema(t *testing.T) {
	t.Parallel()

	s := NewSchema()
	s.AddIntField("a")
	s.AddStringField("b", 10)
	s.AddField("c", SCHEMA_TYPE_INTEGER, 0)

	assert.Equal(t, []string{"a", "b", "c"}, s.Fields())

	typA, _ := s.Type("a")
	lenA, _ := s.Length("a")
	assert.Equal(t, SCHEMA_TYPE_INTEGER, typA)
	assert.Equal(t, 0, lenA)

	typB, _ := s.Type("b")
	lenB, _ := s.Length("b")
	assert.Equal(t, SCHEMA_TYPE_VARCHAR, typB)
	assert.Equal(t, 10, lenB)

	typC, _ := s.Type("c")
	lenC, _ := s.Length("c")
	assert.Equal(t, SCHEMA_TYPE_INTEGER, typC)
	assert.Equal(t, 0, lenC)

	s2 := NewSchema()
	s2.AddAll(s)

	assert.Equal(t, []string{"a", "b", "c"}, s2.Fields())
}
