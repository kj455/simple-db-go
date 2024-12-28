package plan

import (
	"errors"
	"fmt"

	"github.com/kj455/simple-db/pkg/metadata"
	"github.com/kj455/simple-db/pkg/parse"
	"github.com/kj455/simple-db/pkg/tx"
)

type BasicQueryPlanner struct {
	mdMgr metadata.MetadataMgr
}

func NewBasicQueryPlanner(mdMgr metadata.MetadataMgr) *BasicQueryPlanner {
	return &BasicQueryPlanner{
		mdMgr: mdMgr,
	}
}

// CreatePlan creates a query plan for the given query data.
func (bp *BasicQueryPlanner) CreatePlan(data *parse.QueryData, tx tx.Transaction) (Plan, error) {
	plans := make([]Plan, 0, len(data.Tables))
	for _, table := range data.Tables {
		viewDef, err := bp.mdMgr.GetViewDef(table, tx)
		if err != nil && errors.Is(err, metadata.ErrViewNotFound) {
			return nil, fmt.Errorf("planner: failed to get view definition for %s: %v", table, err)
		}
		if isTable := errors.Is(err, metadata.ErrViewNotFound); isTable {
			plan, err := NewTablePlan(tx, table, bp.mdMgr)
			if err != nil {
				return nil, fmt.Errorf("planner: failed to create table plan for %s: %v", table, err)
			}
			plans = append(plans, plan)
			continue
		}
		parser := parse.NewParser(viewDef)
		viewData, err := parser.Query()
		if err != nil {
			return nil, fmt.Errorf("planner: failed to parse view definition for %s: %v", table, err)
		}
		plan, err := bp.CreatePlan(viewData, tx)
		if err != nil {
			return nil, fmt.Errorf("planner: failed to create view plan for %s: %v", table, err)
		}
		plans = append(plans, plan)
	}

	if len(plans) == 0 {
		return nil, errors.New("planner: no tables or views in query")
	}

	plan := plans[0]
	var err error
	for i := 1; i < len(plans); i++ {
		plan, err = NewProductPlan(plan, plans[i])
		if err != nil {
			return nil, fmt.Errorf("planner: failed to create product plan: %v", err)
		}
	}
	plan = NewSelectPlan(plan, data.Pred)
	plan, err = NewProjectPlan(plan, data.Fields)
	if err != nil {
		return nil, fmt.Errorf("planner: failed to create project plan: %v", err)
	}

	return plan, nil
}
