package query

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/constant"
	"github.com/kj455/simple-db/pkg/record"
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

func (s *SelectScan) GetInt(field string) (int, error) {
	return s.scan.GetInt(field)
}

func (s *SelectScan) GetString(field string) (string, error) {
	return s.scan.GetString(field)
}

func (s *SelectScan) GetVal(field string) (*constant.Const, error) {
	return s.scan.GetVal(field)
}

func (s *SelectScan) HasField(field string) bool {
	return s.scan.HasField(field)
}

func (s *SelectScan) Close() {
	s.scan.Close()
}

func (s *SelectScan) SetInt(field string, val int) error {
	us, ok := s.scan.(UpdatableScan)
	if !ok {
		return fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.SetInt(field, val)
}

func (s *SelectScan) SetString(field string, val string) error {
	us, ok := s.scan.(UpdatableScan)
	if !ok {
		return fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.SetString(field, val)
}

func (s *SelectScan) SetVal(field string, val *constant.Const) error {
	us, ok := s.scan.(UpdatableScan)
	if !ok {
		return fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.SetVal(field, val)
}

func (s *SelectScan) Delete() error {
	us, ok := s.scan.(UpdatableScan)
	if !ok {
		return fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.Delete()
}

func (s *SelectScan) Insert() error {
	us, ok := s.scan.(UpdatableScan)
	if !ok {
		return fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.Insert()
}

func (s *SelectScan) GetRID() record.RID {
	us := s.scan.(UpdatableScan)
	return us.GetRID()
}

func (s *SelectScan) MoveToRID(rid record.RID) error {
	us, ok := s.scan.(UpdatableScan)
	if !ok {
		return fmt.Errorf("query: scan is not an UpdateScan")
	}
	return us.MoveToRID(rid)
}
