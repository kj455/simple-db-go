package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	t.Run("c = 1", func(t *testing.T) {
		lex := NewLexer("c = 1")
		assert.True(t, lex.MatchId())

		l, _ := lex.EatId()
		lex.EatDelim('=')
		r, _ := lex.EatIntConstant()

		assert.Equal(t, "c", l)
		assert.Equal(t, 1, r)
	})
	t.Run("1 = c", func(t *testing.T) {
		lex := NewLexer("1 = c")

		l, _ := lex.EatIntConstant()
		lex.EatDelim('=')
		r, _ := lex.EatId()

		assert.Equal(t, 1, l)
		assert.Equal(t, "c", r)
	})
	t.Run("foo = 1", func(t *testing.T) {
		lex := NewLexer("foo = 1")
		assert.True(t, lex.MatchId())

		l, _ := lex.EatId()
		lex.EatDelim('=')
		r, _ := lex.EatIntConstant()

		assert.Equal(t, "foo", l)
		assert.Equal(t, 1, r)
	})
	t.Run("select a from foo", func(t *testing.T) {
		lex := NewLexer("select a from foo")

		err := lex.EatKeyword("select")
		assert.NoError(t, err)

		fld, _ := lex.EatId()
		assert.Equal(t, "a", fld)

		lex.EatKeyword("from")
		tbl, _ := lex.EatId()

		assert.Equal(t, "foo", tbl)
	})
}
