package constant

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstant(t *testing.T) {
	t.Run("NewConstant", func(t *testing.T) {
		c, _ := NewConstant(KIND_INT, 42)
		assert.Equal(t, 42, c.AsInt())
		c, _ = NewConstant(KIND_STR, "hello")
		assert.Equal(t, "hello", c.AsString())
		_, err := NewConstant(KIND_INT, "hello")
		assert.Error(t, err)
	})
	t.Run("Equals", func(t *testing.T) {
		c1, _ := NewConstant(KIND_INT, 42)
		c2, _ := NewConstant(KIND_INT, 42)
		c3, _ := NewConstant(KIND_INT, 43)
		c4, _ := NewConstant(KIND_STR, "hello")
		assert.True(t, c1.Equals(c2))
		assert.False(t, c1.Equals(c3))
		assert.False(t, c1.Equals(c4))
	})
	t.Run("CompareTo", func(t *testing.T) {
		c1, _ := NewConstant(KIND_INT, 42)
		c2, _ := NewConstant(KIND_INT, 42)
		c3, _ := NewConstant(KIND_INT, 43)
		c4, _ := NewConstant(KIND_STR, "hello")
		assert.Equal(t, 0, c1.CompareTo(c2))
		assert.Equal(t, -1, c1.CompareTo(c3))
		assert.Equal(t, 1, c3.CompareTo(c1))
		assert.Equal(t, 0, c4.CompareTo(c4))
	})
}
