package file

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

const defaultCharset = "us-ascii"

type PageImpl struct {
	buf     *bytes.Buffer
	charset string
}

func NewPage(blockSize int) *PageImpl {
	return &PageImpl{
		buf:     bytes.NewBuffer(make([]byte, blockSize)),
		charset: defaultCharset,
	}
}

func NewPageFromBytes(data []byte) *PageImpl {
	return &PageImpl{
		buf:     bytes.NewBuffer(data),
		charset: defaultCharset,
	}
}

func (p *PageImpl) GetInt(offset int) uint32 {
	data := p.buf.Bytes()[offset : offset+4]
	return binary.BigEndian.Uint32(data)
}

func (p *PageImpl) SetInt(offset int, value uint32) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(value))
	copy(p.buf.Bytes()[offset:], data)
}

func (p *PageImpl) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.buf.Bytes()[offset+4 : offset+4+int(length)]
}

func (p *PageImpl) SetBytes(offset int, value []byte) {
	p.SetInt(offset, uint32(len(value)))
	copy(p.buf.Bytes()[offset+4:], value)
}

func (p *PageImpl) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *PageImpl) SetString(offset int, value string) {
	p.SetBytes(offset, []byte(value))
}

func (p *PageImpl) Contents() *bytes.Buffer {
	return p.buf
}

func MaxLength(strLen int) int {
	bytesPerChar := utf8.UTFMax
	return 4 + strLen*bytesPerChar
}
