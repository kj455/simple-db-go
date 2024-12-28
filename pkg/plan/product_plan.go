package plan

import (
	"fmt"

	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
)

type ProductPlan struct {
	p1, p2 Plan
	schema record.Schema
}

func NewProductPlan(p1, p2 Plan) (*ProductPlan, error) {
	schema := record.NewSchema()
	if err := schema.AddAll(p1.Schema()); err != nil {
		return nil, fmt.Errorf("plan: failed to add schema: %v", err)
	}
	if err := schema.AddAll(p2.Schema()); err != nil {
		return nil, fmt.Errorf("plan: failed to add schema: %v", err)
	}
	return &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}, nil
}

func (pp *ProductPlan) Open() (query.Scan, error) {
	s1, err := pp.p1.Open()
	if err != nil {
		return nil, fmt.Errorf("plan: failed to open scan: %v", err)
	}
	s2, err := pp.p2.Open()
	if err != nil {
		return nil, fmt.Errorf("plan: failed to open scan: %v", err)
	}
	sc, err := query.NewProductScan(s1, s2)
	if err != nil {
		return nil, fmt.Errorf("plan: failed to open product scan: %v", err)
	}
	return sc, nil
}

func (pp *ProductPlan) BlocksAccessed() int {
	return pp.p1.BlocksAccessed() + (pp.p1.RecordsOutput() * pp.p2.BlocksAccessed())
}

func (pp *ProductPlan) RecordsOutput() int {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

func (pp *ProductPlan) DistinctValues(field string) int {
	if pp.p1.Schema().HasField(field) {
		return pp.p1.DistinctValues(field)
	}
	return pp.p2.DistinctValues(field)
}

func (pp *ProductPlan) Schema() record.Schema {
	return pp.schema
}
