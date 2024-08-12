package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPredParser(t *testing.T) {
	t.Parallel()
	tests := []string{
		"foo = 1",
		"foo = 1 and bar = 2",
	}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			t.Parallel()
			p := NewPredParser(tt)
			err := p.Predicate()
			assert.NoError(t, err)
		})
	}
}
