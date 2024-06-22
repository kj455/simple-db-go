package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/stretchr/testify/assert"
)

func TestNewLogRecord(t *testing.T) {
	const size = 128
	t.Parallel()
	tests := []struct {
		name   string
		args   []byte
		expect Op
	}{
		{
			name: "CHECKPOINT",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(CHECKPOINT))
				return p.Contents().Bytes()
			}(),
			expect: CHECKPOINT,
		},
		{
			name: "START",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(START))
				return p.Contents().Bytes()
			}(),
			expect: START,
		},
		{
			name: "COMMIT",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(COMMIT))
				return p.Contents().Bytes()
			}(),
			expect: COMMIT,
		},
		{
			name: "ROLLBACK",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(ROLLBACK))
				return p.Contents().Bytes()
			}(),
			expect: ROLLBACK,
		},
		{
			name: "SET_INT",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(SET_INT))
				return p.Contents().Bytes()
			}(),
			expect: SET_INT,
		},
		{
			name: "SET_STRING",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(SET_STRING))
				return p.Contents().Bytes()
			}(),
			expect: SET_STRING,
		},
		{
			name: "default",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(100))
				return p.Contents().Bytes()
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewLogRecord(tt.args)
			if got != nil {
				assert.Equal(t, tt.expect, got.Op())
				return
			}
			assert.Nil(t, got)
		})
	}
}
