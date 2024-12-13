package transaction

import (
	"testing"

	"github.com/kj455/db/pkg/file"
	"github.com/stretchr/testify/assert"
)

func TestNewLogRecord(t *testing.T) {
	t.Parallel()
	const size = 128
	tests := []struct {
		name      string
		args      []byte
		expect    Op
		expectErr bool
	}{
		{
			name: "CHECKPOINT",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(OP_CHECKPOINT))
				return p.Contents().Bytes()
			}(),
			expect: OP_CHECKPOINT,
		},
		{
			name: "START",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(OP_START))
				return p.Contents().Bytes()
			}(),
			expect: OP_START,
		},
		{
			name: "COMMIT",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(OP_COMMIT))
				return p.Contents().Bytes()
			}(),
			expect: OP_COMMIT,
		},
		{
			name: "ROLLBACK",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(OP_ROLLBACK))
				return p.Contents().Bytes()
			}(),
			expect: OP_ROLLBACK,
		},
		{
			name: "SET_INT",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(OP_SET_INT))
				return p.Contents().Bytes()
			}(),
			expect: OP_SET_INT,
		},
		{
			name: "SET_STRING",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(OP_SET_STRING))
				return p.Contents().Bytes()
			}(),
			expect: OP_SET_STRING,
		},
		{
			name: "default",
			args: func() []byte {
				p := file.NewPage(size)
				p.SetInt(0, uint32(100))
				return p.Contents().Bytes()
			}(),
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewLogRecord(tt.args)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.Equal(t, tt.expect, got.Op())
		})
	}
}
