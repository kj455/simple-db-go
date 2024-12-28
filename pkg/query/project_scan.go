package query

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/constant"
)

type ProjectScan struct {
	scan   Scan
	fields []string
}

func NewProjectScan(scan Scan, fields []string) *ProjectScan {
	return &ProjectScan{
		scan:   scan,
		fields: fields,
	}
}

func (ps *ProjectScan) BeforeFirst() error {
	return ps.scan.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.scan.Next()
}

func (ps *ProjectScan) GetInt(field string) (int, error) {
	if !ps.HasField(field) {
		return 0, fmt.Errorf("query: field %s not found", field)
	}
	return ps.scan.GetInt(field)
}

func (ps *ProjectScan) GetString(field string) (string, error) {
	if !ps.HasField(field) {
		return "", fmt.Errorf("query: field %s not found", field)
	}
	return ps.scan.GetString(field)
}

func (ps *ProjectScan) GetVal(field string) (*constant.Const, error) {
	if !ps.HasField(field) {
		return nil, fmt.Errorf("query: field %s not found", field)
	}
	return ps.scan.GetVal(field)
}

func (ps *ProjectScan) HasField(field string) bool {
	for _, f := range ps.fields {
		if f == field {
			return true
		}
	}
	return false
}

func (ps *ProjectScan) Close() {
	ps.scan.Close()
}
