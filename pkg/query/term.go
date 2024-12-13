package query

import (
	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/record"
)

type Term struct {
	lhs, rhs Expression
}

// NewTerm creates a new Term instance with two expressions
func NewTerm(lhs, rhs Expression) *Term {
	return &Term{
		lhs: lhs,
		rhs: rhs,
	}
}

func (t *Term) IsSatisfied(s Scan) (bool, error) {
	lhsVal, err := t.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	rhsVal, err := t.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	return lhsVal.Equals(rhsVal), nil
}

// ReductionFactor calculates the extent to which selecting on the predicate reduces the number of records output by a query.
func (t *Term) ReductionFactor(p PlanInfo) int {
	var lhsName, rhsName string
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		rhsName = t.rhs.AsFieldName()
		return max(p.DistinctValues(lhsName), p.DistinctValues(rhsName))
	}
	if t.lhs.IsFieldName() {
		lhsName = t.lhs.AsFieldName()
		return p.DistinctValues(lhsName)
	}
	if t.rhs.IsFieldName() {
		rhsName = t.rhs.AsFieldName()
		return p.DistinctValues(rhsName)
	}
	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		return 1
	}
	return int(^uint(0) >> 1) // Max int value
}

func (t *Term) FindConstantEquivalence(field string) (*constant.Const, bool) {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == field && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant(), true
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == field && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant(), true
	}
	return nil, false
}

func (t *Term) FindFieldEquivalence(field string) (string, bool) {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == field && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName(), true
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == field && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName(), true
	}
	return "", false
}

func (t *Term) CanApply(sch record.Schema) bool {
	return t.lhs.CanApply(sch) && t.rhs.CanApply(sch)
}

func (t *Term) String() string {
	return t.lhs.ToString() + "=" + t.rhs.ToString()
}
