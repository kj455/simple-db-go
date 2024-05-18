package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	blockSize := 4096
	page := NewPage(blockSize)
	assert.NotNil(t, page)
	assert.Equal(t, blockSize, page.buf.Len())
	assert.Equal(t, defaultCharset, page.charset)
}

func TestNewPageFromBytes(t *testing.T) {
	blockSize := 4096
	data := make([]byte, blockSize)
	page := NewPageFromBytes(data)
	assert.NotNil(t, page)
	assert.Equal(t, blockSize, page.buf.Len())
	assert.Equal(t, defaultCharset, page.charset)
}

func TestPageInt(t *testing.T) {
	page := NewPage(4096)
	offset := 0
	value := uint32(42)
	page.SetInt(offset, value)
	assert.Equal(t, value, page.GetInt(offset))
}

func TestPageBytes(t *testing.T) {
	page := NewPage(4096)
	offset := 0
	value := []byte("hello")
	page.SetBytes(offset, value)
	assert.Equal(t, value, page.GetBytes(offset))
}

func TestPageString(t *testing.T) {
	page := NewPage(4096)
	offset := 0
	value := "hello"
	page.SetString(offset, value)
	assert.Equal(t, value, page.GetString(offset))
}

func TestMaxLength(t *testing.T) {
	assert.Equal(t, 4*1024+4, MaxLength(1024))
}
