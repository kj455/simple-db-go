package file

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

const defaultCharset = "us-ascii"

type Page struct {
	buf     *bytes.Buffer
	charset string
}

func NewPage(blockSize int) *Page {
	return &Page{
		buf:     bytes.NewBuffer(make([]byte, blockSize)),
		charset: defaultCharset,
	}
}

func NewPageFromBytes(data []byte) *Page {
	return &Page{
		buf:     bytes.NewBuffer(data),
		charset: defaultCharset,
	}
}

func (p *Page) GetInt(offset int) uint32 {
	return binary.BigEndian.Uint32(p.buf.Bytes()[offset:])
}

func (p *Page) SetInt(offset int, value uint32) {
	binary.BigEndian.PutUint32(p.buf.Bytes()[offset:], value)
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.buf.Bytes()[offset+4 : offset+4+int(length)]
}

func (p *Page) SetBytes(offset int, value []byte) {
	p.SetInt(offset, uint32(len(value)))
	copy(p.buf.Bytes()[offset+4:], value)
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, value string) {
	p.SetBytes(offset, []byte(value))
}

func (p *Page) Contents() *bytes.Buffer {
	return p.buf
}

func MaxLength(strLen int) int {
	bytesPerChar := utf8.UTFMax
	return 4 + strLen*bytesPerChar
}
