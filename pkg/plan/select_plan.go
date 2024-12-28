package plan

import (
	"fmt"

	"github.com/kj455/simple-db/pkg/query"
	"github.com/kj455/simple-db/pkg/record"
)

type SelectPlan struct {
	plan Plan
	pred query.Predicate
}

func NewSelectPlan(p Plan, pred query.Predicate) *SelectPlan {
	return &SelectPlan{
		plan: p,
		pred: pred,
	}
}

func (sp *SelectPlan) Open() (query.Scan, error) {
	scan, err := sp.plan.Open()
	if err != nil {
		return nil, fmt.Errorf("plan: failed to open scan: %v", err)
	}
	return query.NewSelectScan(scan, sp.pred), nil
}

func (sp *SelectPlan) BlocksAccessed() int {
	return sp.plan.BlocksAccessed()
}

func (sp *SelectPlan) RecordsOutput() int {
	return sp.plan.RecordsOutput()
}

func (sp *SelectPlan) DistinctValues(field string) int {
	if _, ok := sp.pred.FindConstantEquivalence(field); ok {
		return 1
	}
	if field2, ok := sp.pred.FindFieldEquivalence(field); ok {
		return min(sp.plan.DistinctValues(field), sp.plan.DistinctValues(field2))
	}
	return sp.plan.DistinctValues(field)
}

func (sp *SelectPlan) Schema() record.Schema {
	return sp.plan.Schema()
}
