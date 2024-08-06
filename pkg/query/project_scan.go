package query

import (
	"fmt"

	"github.com/kj455/db/pkg/constant"
)

type ProjectScan struct {
	s      Scan
	fields []string
}

func NewProjectScan(s Scan, fields []string) *ProjectScan {
	return &ProjectScan{
		s:      s,
		fields: fields,
	}
}

func (ps *ProjectScan) BeforeFirst() error {
	return ps.s.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.s.Next()
}

func (ps *ProjectScan) GetInt(fldname string) (int, error) {
	if ps.HasField(fldname) {
		return ps.s.GetInt(fldname)
	}
	return 0, fmt.Errorf("query: field %s not found", fldname)
}

func (ps *ProjectScan) GetString(fldname string) (string, error) {
	if ps.HasField(fldname) {
		return ps.s.GetString(fldname)
	}
	return "", fmt.Errorf("query: field %s not found", fldname)
}

func (ps *ProjectScan) GetVal(fldname string) (*constant.Const, error) {
	if ps.HasField(fldname) {
		return ps.s.GetVal(fldname)
	}
	return nil, fmt.Errorf("query: field %s not found", fldname)
}

func (ps *ProjectScan) HasField(fldname string) bool {
	for _, field := range ps.fields {
		if field == fldname {
			return true
		}
	}
	return false
}

func (ps *ProjectScan) Close() {
	ps.s.Close()
}
