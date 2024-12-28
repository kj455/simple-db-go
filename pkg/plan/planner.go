package plan

import (
	"fmt"

	"github.com/kj455/db/pkg/parse"
	"github.com/kj455/db/pkg/tx"
)

type Planner struct {
	queryPlanner  *BasicQueryPlanner
	updatePlanner *BasicUpdatePlanner
}

func NewPlanner(qp *BasicQueryPlanner, up *BasicUpdatePlanner) *Planner {
	return &Planner{
		queryPlanner:  qp,
		updatePlanner: up,
	}
}

func (p *Planner) CreateQueryPlan(cmd string, tx tx.Transaction) (Plan, error) {
	parser := parse.NewParser(cmd)
	data, err := parser.Query()
	if err != nil {
		return nil, fmt.Errorf("planner: failed to parse query: %v", err)
	}
	return p.queryPlanner.CreatePlan(data, tx)
}

func (p *Planner) ExecuteUpdate(cmd string, tx tx.Transaction) (int, error) {
	parser := parse.NewParser(cmd)
	data, err := parser.UpdateCmd()
	if err != nil {
		return 0, fmt.Errorf("planner: failed to parse update: %v", err)
	}
	switch data := data.(type) {
	case *parse.InsertData:
		return p.updatePlanner.ExecuteInsert(*data, tx)
	case *parse.DeleteData:
		return p.updatePlanner.ExecuteDelete(*data, tx)
	case *parse.ModifyData:
		return p.updatePlanner.ExecuteModify(*data, tx)
	case *parse.CreateTableData:
		return p.updatePlanner.ExecuteCreateTable(*data, tx)
	case *parse.CreateViewData:
		return p.updatePlanner.ExecuteCreateView(*data, tx)
	case *parse.CreateIndexData:
		return p.updatePlanner.ExecuteCreateIndex(*data, tx)
	default:
		return 0, fmt.Errorf("planner: unknown update type %T", data)
	}
}
