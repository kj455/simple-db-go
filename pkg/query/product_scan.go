package query

import (
	"fmt"

	"github.com/kj455/db/pkg/constant"
)

// ProductScan corresponds to the product relational algebra operator.
type ProductScan struct {
	s1, s2 Scan
}

// NewProductScan creates a product scan having the two underlying scans.
func NewProductScan(s1, s2 Scan) (*ProductScan, error) {
	p := &ProductScan{
		s1: s1,
		s2: s2,
	}
	if err := p.BeforeFirst(); err != nil {
		return nil, err
	}
	return p, nil
}

// BeforeFirst positions the scan before its first record. In particular, the LHS scan is positioned at its first record, and the RHS scan is positioned before its first record.
func (p *ProductScan) BeforeFirst() error {
	if err := p.s1.BeforeFirst(); err != nil {
		return err
	}
	p.s1.Next()
	return p.s2.BeforeFirst()
}

// Next moves the scan to the next record. The method moves to the next RHS record, if possible. Otherwise, it moves to the next LHS record and the first RHS record. If there are no more LHS records, the method returns false.
func (p *ProductScan) Next() bool {
	if p.s2.Next() {
		return true
	}
	if err := p.s2.BeforeFirst(); err != nil {
		return false
	}
	return p.s2.Next() && p.s1.Next()
}

// GetInt returns the integer value of the specified field. The value is obtained from whichever scan contains the field.
func (p *ProductScan) GetInt(field string) (int, error) {
	fmt.Println("field:", field)
	if p.s1.HasField(field) {
		v, _ := p.s1.GetInt(field)
		fmt.Println("get from s1:", v)
		return p.s1.GetInt(field)
	}
	v, _ := p.s2.GetInt(field)
	fmt.Println("get from s2:", v)
	return p.s2.GetInt(field)
}

// GetString returns the string value of the specified field. The value is obtained from whichever scan contains the field.
func (p *ProductScan) GetString(field string) (string, error) {
	if p.s1.HasField(field) {
		return p.s1.GetString(field)
	}
	return p.s2.GetString(field)
}

// GetVal returns the value of the specified field. The value is obtained from whichever scan contains the field.
func (p *ProductScan) GetVal(field string) (*constant.Const, error) {
	if p.s1.HasField(field) {
		return p.s1.GetVal(field)
	}
	return p.s2.GetVal(field)
}

// HasField returns true if the specified field is in either of the underlying scans.
func (p *ProductScan) HasField(field string) bool {
	return p.s1.HasField(field) || p.s2.HasField(field)
}

// Close closes both underlying scans.
func (p *ProductScan) Close() {
	p.s1.Close()
	p.s2.Close()
}
