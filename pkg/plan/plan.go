package plan

import (
	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
)

type Plan interface {
	Open() (query.Scan, error)
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(field string) int
	Schema() record.Schema
}
