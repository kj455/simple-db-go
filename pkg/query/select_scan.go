package query

import (
	"fmt"

	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/record"
)

type SelectScan struct {
	scan Scan
	pred Predicate
}

// NewSelectScan creates a new SelectScan instance
func NewSelectScan(s Scan, pred Predicate) *SelectScan {
	return &SelectScan{
		scan: s,
		pred: pred,
	}
}

// BeforeFirst positions the scan before its first record
func (s *SelectScan) BeforeFirst() error {
	return s.scan.BeforeFirst()
}

// Next moves the scan to the next record and returns true if there is such a record
func (s *SelectScan) Next() bool {
	for s.scan.Next() {
		if ok, err := s.pred.IsSatisfied(s.scan); ok && err == nil {
			return true
		}
	}
	return false
}

func (s *SelectScan) GetInt(fldname string) (int, error) {
	return s.scan.GetInt(fldname)
}

func (s *SelectScan) GetString(fldname string) (string, error) {
	return s.scan.GetString(fldname)
}

func (s *SelectScan) GetVal(fldname string) (*constant.Const, error) {
	return s.scan.GetVal(fldname)
}

func (s *SelectScan) HasField(fldname string) bool {
	return s.scan.HasField(fldname)
}

func (s *SelectScan) Close() {
	s.scan.Close()
}

func (s *SelectScan) SetInt(fldname string, val int) error {
	us := s.scan.(UpdateScan)
	return us.SetInt(fldname, val)
}

func (s *SelectScan) SetString(fldname string, val string) error {
	us := s.scan.(UpdateScan)
	return us.SetString(fldname, val)
}

func (s *SelectScan) SetVal(fldname string, val *constant.Const) error {
	us := s.scan.(UpdateScan)
	return us.SetVal(fldname, val)
}

func (s *SelectScan) Delete() error {
	us := s.scan.(UpdateScan)
	return us.Delete()
}

func (s *SelectScan) Insert() error {
	us := s.scan.(UpdateScan)
	return us.Insert()
}

func (s *SelectScan) GetRid() (record.RID, error) {
	us, ok := s.scan.(UpdateScan)
	if !ok {
		return nil, fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.GetRID(), nil
}

func (s *SelectScan) MoveToRid(rid record.RID) error {
	us := s.scan.(UpdateScan)
	return us.MoveToRID(rid)
}
