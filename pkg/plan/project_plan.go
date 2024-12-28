package plan

import (
	"fmt"

	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
)

type ProjectPlan struct {
	plan   Plan
	schema record.Schema
}

func NewProjectPlan(p Plan, fields []string) (*ProjectPlan, error) {
	schema := record.NewSchema()
	for _, field := range fields {
		if err := schema.Add(field, p.Schema()); err != nil {
			return nil, fmt.Errorf("plan: failed to add field %s: %v", field, err)
		}
	}
	return &ProjectPlan{
		plan:   p,
		schema: schema,
	}, nil
}

func (pp *ProjectPlan) Open() (query.Scan, error) {
	scan, err := pp.plan.Open()
	if err != nil {
		return nil, fmt.Errorf("plan: failed to open scan: %v", err)
	}
	return query.NewProjectScan(scan, pp.schema.Fields()), nil
}

func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.plan.BlocksAccessed()
}

func (pp *ProjectPlan) RecordsOutput() int {
	return pp.plan.RecordsOutput()
}

func (pp *ProjectPlan) DistinctValues(field string) int {
	return pp.plan.DistinctValues(field)
}

func (pp *ProjectPlan) Schema() record.Schema {
	return pp.schema
}
