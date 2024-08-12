package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser_String(t *testing.T) {
	t.Parallel()
	t.Run("select", func(t *testing.T) {
		t.Parallel()
		s := "select foo, bar from tests where foo=1"
		p := NewParser(s)
		q, err := p.Query()
		assert.NoError(t, err)
		assert.Equal(t, s, q.String())
	})
	t.Run("insert", func(t *testing.T) {
		t.Parallel()
		s := "insert into tests(foo, bar) values(1, 2)"
		p := NewParser(s)
		data, err := p.Insert()
		assert.NoError(t, err)
		assert.Equal(t, s, data.String())
	})
	t.Run("delete", func(t *testing.T) {
		t.Parallel()
		s := "delete from tests where foo=1"
		p := NewParser(s)
		data, err := p.Delete()
		assert.NoError(t, err)
		assert.Equal(t, s, data.String())
	})
	t.Run("update", func(t *testing.T) {
		t.Parallel()
		s := "update tests set a = 1"
		p := NewParser(s)
		data, err := p.Modify()
		assert.NoError(t, err)
		assert.Equal(t, s, data.String())
	})
	t.Run("create", func(t *testing.T) {
		t.Run("table", func(t *testing.T) {
			t.Parallel()
			s := "create table tests(foo int, bar varchar(255))"
			p := NewParser(s)
			p.lex.EatKeyword("create")
			data, err := p.CreateTable()
			assert.NoError(t, err)
			assert.Equal(t, s, data.String())
		})
		t.Run("view", func(t *testing.T) {
			t.Parallel()
			s := "create view tests as select * from tests"
			p := NewParser(s)
			p.lex.EatKeyword("create")
			data, err := p.CreateView()
			assert.NoError(t, err)
			assert.Equal(t, s, data.String())
		})
		t.Run("index", func(t *testing.T) {
			t.Parallel()
			s := "create index idx on tests(foo)"
			p := NewParser(s)
			p.lex.EatKeyword("create")
			data, err := p.CreateIndex()
			assert.NoError(t, err)
			assert.Equal(t, s, data.String())
		})
	})
}
