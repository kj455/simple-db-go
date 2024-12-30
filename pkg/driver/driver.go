package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/kj455/simple-db/pkg/buffer"
	"github.com/kj455/simple-db/pkg/file"
	"github.com/kj455/simple-db/pkg/log"
	"github.com/kj455/simple-db/pkg/metadata"
	"github.com/kj455/simple-db/pkg/plan"
	"github.com/kj455/simple-db/pkg/query"
	"github.com/kj455/simple-db/pkg/tx"
)

func init() {
	sql.Register("simple", NewSimpleDriver())
}

type SimpleDriver struct {
}

func NewSimpleDriver() *SimpleDriver {
	return &SimpleDriver{}
}

func (d *SimpleDriver) Open(name string) (driver.Conn, error) {
	return NewConn(name)
}

type Conn struct {
	fileMgr file.FileMgr
	bufMgr  buffer.BufferMgr
	logMgr  log.LogMgr
	tx      tx.Transaction
	mdMgr   metadata.MetadataMgr
	planner *plan.Planner
}

const dir = "./.tmp"

func NewConn(name string) (*Conn, error) {
	const (
		buffNum     = 8
		blockSize   = 4096
		logFileName = "simple-db-conn-log"
	)
	fileMgr := file.NewFileMgr(dir, blockSize)
	logMgr, err := log.NewLogMgr(fileMgr, logFileName)
	if err != nil {
		return nil, fmt.Errorf("driver: failed to create log manager: %v", err)
	}
	buffs := make([]buffer.Buffer, buffNum)
	for i := 0; i < buffNum; i++ {
		buffs[i] = buffer.NewBuffer(fileMgr, logMgr, blockSize)
	}
	bm := buffer.NewBufferMgr(buffs)
	txNumGen := tx.NewTxNumberGenerator()
	tx, err := tx.NewTransaction(fileMgr, logMgr, bm, txNumGen)
	if err != nil {
		return nil, fmt.Errorf("driver: failed to create transaction: %v", err)
	}
	isNew := fileMgr.IsNew()
	if !isNew {
		if err := tx.Recover(); err != nil {
			return nil, fmt.Errorf("driver: failed to recover transaction: %v", err)
		}
	}
	mdMgr, err := metadata.NewMetadataMgr(tx)
	if err != nil {
		return nil, fmt.Errorf("driver: failed to create metadata manager: %v", err)
	}
	qp := plan.NewBasicQueryPlanner(mdMgr)
	up := plan.NewBasicUpdatePlanner(mdMgr)
	planner := plan.NewPlanner(qp, up)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("driver: failed to commit transaction: %v", err)
	}
	return &Conn{
		fileMgr: fileMgr,
		bufMgr:  bm,
		logMgr:  logMgr,
		tx:      tx,
		mdMgr:   mdMgr,
		planner: planner,
	}, nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("driver: not implemented")
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return NewSimpleStmt(query, c), nil
}

type Stmt struct {
	conn  *Conn
	query string
}

func NewSimpleStmt(query string, conn *Conn) *Stmt {
	return &Stmt{
		query: query,
		conn:  conn,
	}
}

func (s *Stmt) Close() error {
	return s.conn.tx.Commit()
}

func (s *Stmt) NumInput() int {
	return -1
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	n, err := s.conn.planner.ExecuteUpdate(s.query, s.conn.tx)
	if err != nil {
		return nil, err
	}
	return Result{n: n}, nil
}

type Result struct {
	n int
}

func (r Result) LastInsertId() (int64, error) {
	return 0, errors.New("driver: not implemented")
}

func (r Result) RowsAffected() (int64, error) {
	return int64(r.n), nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	p, err := s.conn.planner.CreateQueryPlan(s.query, s.conn.tx)
	if err != nil {
		return nil, err
	}
	scan, err := p.Open()
	if err != nil {
		return nil, fmt.Errorf("driver: failed to open plan: %v", err)
	}
	fields := p.Schema().Fields()
	return NewSimpleRows(scan, fields), nil
}

type Rows struct {
	scan   query.Scan
	fields []string
}

func NewSimpleRows(scan query.Scan, fields []string) *Rows {
	return &Rows{
		scan:   scan,
		fields: fields,
	}
}

func (r *Rows) Columns() []string {
	return r.fields
}

func (r *Rows) Close() error {
	r.scan.Close()
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if !r.scan.Next() {
		return driver.ErrSkip
	}
	for i, field := range r.fields {
		val, err := r.scan.GetVal(field)
		if err != nil {
			return fmt.Errorf("driver: failed to get value: %v", err)
		}
		dest[i] = val.AnyValue()
	}
	return nil
}
