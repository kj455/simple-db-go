package parse

import (
	"fmt"

	"github.com/kj455/db/pkg/constant"
	"github.com/kj455/db/pkg/query"
	"github.com/kj455/db/pkg/record"
)

// Parser is the SimpleDB parser.
type Parser struct {
	lexer *Lexer
}

func NewParser(input string) *Parser {
	return &Parser{lexer: NewLexer(input)}
}

func (p *Parser) Field() (string, error) {
	return p.lexer.EatId()
}

func (p *Parser) Constant() (*constant.Const, error) {
	if p.lexer.MatchStringConstant() {
		str, err := p.lexer.EatStringConstant()
		if err != nil {
			return nil, fmt.Errorf("parse: invalid string constant: %w", err)
		}
		cons, err := constant.NewConstant(constant.KIND_STR, str)
		if err != nil {
			return nil, fmt.Errorf("parse: invalid string constant: %w", err)
		}
		return cons, nil
	}
	if p.lexer.matchIntConstant() {
		num, err := p.lexer.EatIntConstant()
		if err != nil {
			return nil, fmt.Errorf("parse: invalid integer constant: %w", err)
		}
		cons, err := constant.NewConstant(constant.KIND_INT, num)
		if err != nil {
			return nil, fmt.Errorf("parse: invalid integer constant: %w", err)
		}
		return cons, nil
	}
	return nil, fmt.Errorf("parse: invalid constant")
}

// Expression parses and returns an expression.
func (p *Parser) Expression() (query.Expression, error) {
	if p.lexer.MatchId() {
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		return query.NewFieldExpression(field), nil
	}
	constant, err := p.Constant()
	if err != nil {
		return nil, err
	}
	return query.NewConstantExpression(constant), nil
}

// Term parses and returns a term.
func (p *Parser) Term() (*query.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim('='); err != nil {
		return nil, fmt.Errorf("expected '=' in term: %w", err)
	}
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(lhs, rhs), nil
}

func (p *Parser) Predicate() (*query.PredicateImpl, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	predicate := query.NewPredicate(term)
	for p.lexer.MatchKeyword("and") {
		if err := p.lexer.EatKeyword("and"); err != nil {
			return nil, err
		}
		nextPredicate, err := p.Predicate()
		if err != nil {
			return nil, err
		}
		predicate.ConjoinWith(nextPredicate)
	}
	return predicate, nil
}

// Query parses and returns a query.
func (p *Parser) Query() (*QueryData, error) {
	if err := p.lexer.EatKeyword("select"); err != nil {
		return nil, err
	}
	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("from"); err != nil {
		return nil, err
	}
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}
	pred := &query.PredicateImpl{}
	if p.lexer.MatchKeyword("where") {
		if err := p.lexer.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewQueryData(fields, tables, pred), nil
}

func (p *Parser) selectList() ([]string, error) {
	f, err := p.Field()
	if err != nil {
		return nil, err
	}
	l := []string{f}
	if p.lexer.MatchDelim(',') {
		if err := p.lexer.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.selectList()
		if err != nil {
			return nil, err
		}
		l = append(l, list...)
	}
	return l, nil
}

func (p *Parser) tableList() ([]string, error) {
	id, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	l := []string{id}
	if p.lexer.MatchDelim(',') {
		if err := p.lexer.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.tableList()
		if err != nil {
			return nil, err
		}
		l = append(l, list...)
	}
	return l, nil
}

// UpdateCmd parses and returns an update command.
func (p *Parser) UpdateCmd() (any, error) {
	if p.lexer.MatchKeyword("insert") {
		return p.Insert()
	}
	if p.lexer.MatchKeyword("delete") {
		return p.Delete()
	}
	if p.lexer.MatchKeyword("update") {
		return p.Modify()
	}
	if p.lexer.MatchKeyword("create") {
		return p.create()
	}
	return nil, fmt.Errorf("parse: invalid command")
}

func (p *Parser) create() (any, error) {
	if err := p.lexer.EatKeyword("create"); err != nil {
		return nil, err
	}
	if p.lexer.MatchKeyword("table") {
		return p.CreateTable()
	}
	if p.lexer.MatchKeyword("view") {
		return p.CreateView()
	}
	if p.lexer.MatchKeyword("index") {
		return p.CreateIndex()
	}
	return nil, fmt.Errorf("parse: invalid command")
}

// Delete parses and returns a delete data.
func (p *Parser) Delete() (*DeleteData, error) {
	if err := p.lexer.EatKeyword("delete"); err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("from"); err != nil {
		return nil, err
	}
	table, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	pred := &query.PredicateImpl{}
	if p.lexer.MatchKeyword("where") {
		if err := p.lexer.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(table, pred), nil
}

// Insert parses and returns an insert data.
func (p *Parser) Insert() (*InsertData, error) {
	if err := p.lexer.EatKeyword("insert"); err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("into"); err != nil {
		return nil, err
	}
	table, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim('('); err != nil {
		return nil, err
	}
	fields, err := p.FieldList()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim(')'); err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("values"); err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim('('); err != nil {
		return nil, err
	}
	vals, err := p.constList()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewInsertData(table, fields, vals), nil
}

func (p *Parser) FieldList() ([]string, error) {
	item, err := p.Field()
	if err != nil {
		return nil, err
	}
	list := []string{item}
	for p.lexer.MatchDelim(',') {
		if err := p.lexer.EatDelim(','); err != nil {
			return nil, err
		}
		item, err := p.Field()
		if err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, nil
}

func (p *Parser) constList() ([]*constant.Const, error) {
	cons, err := p.Constant()
	if err != nil {
		return nil, err
	}
	l := []*constant.Const{cons}
	if p.lexer.MatchDelim(',') {
		if err := p.lexer.EatDelim(','); err != nil {
			return nil, err
		}
		list, err := p.constList()
		if err != nil {
			return nil, err
		}
		l = append(l, list...)
	}
	return l, nil
}

// Modify parses and returns a modify data.
func (p *Parser) Modify() (*ModifyData, error) {
	if err := p.lexer.EatKeyword("update"); err != nil {
		return nil, err
	}
	table, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("set"); err != nil {
		return nil, err
	}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim('='); err != nil {
		return nil, err
	}
	expr, err := p.Expression()
	if err != nil {
		return nil, err
	}
	pred := &query.PredicateImpl{}
	if p.lexer.MatchKeyword("where") {
		if err := p.lexer.EatKeyword("where"); err != nil {
			return nil, err
		}
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewModifyData(table, field, expr, pred), nil
}

// CreateTable parses and returns a create table data.
func (p *Parser) CreateTable() (*CreateTableData, error) {
	if err := p.lexer.EatKeyword("table"); err != nil {
		return nil, err
	}
	table, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim('('); err != nil {
		return nil, err
	}
	sch, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewCreateTableData(table, sch), nil
}

func (p *Parser) fieldDefs() (record.Schema, error) {
	schema, err := p.fieldDef()
	if err != nil {
		return nil, err
	}
	if p.lexer.MatchDelim(',') {
		if err := p.lexer.EatDelim(','); err != nil {
			return nil, err
		}
		schema2, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}
		if err := schema.AddAll(schema2); err != nil {
			return nil, err
		}
	}
	return schema, nil
}

func (p *Parser) fieldDef() (record.Schema, error) {
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(field)
}

func (p *Parser) fieldType(field string) (record.Schema, error) {
	schema := record.NewSchema()
	if p.lexer.MatchKeyword("int") {
		if err := p.lexer.EatKeyword("int"); err != nil {
			return nil, err
		}
		schema.AddIntField(field)
		return schema, nil
	}
	if p.lexer.MatchKeyword("varchar") {
		if err := p.lexer.EatKeyword("varchar"); err != nil {
			return nil, err
		}
		if err := p.lexer.EatDelim('('); err != nil {
			return nil, err
		}
		strLen, err := p.lexer.EatIntConstant()
		if err != nil {
			return nil, err
		}
		if err := p.lexer.EatDelim(')'); err != nil {
			return nil, err
		}
		schema.AddStringField(field, strLen)
	}
	return schema, nil
}

// CreateView parses and returns a create view data.
func (p *Parser) CreateView() (*CreateViewData, error) {
	if err := p.lexer.EatKeyword("view"); err != nil {
		return nil, err
	}
	viewname, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("as"); err != nil {
		return nil, err
	}
	qd, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewname, qd), nil
}

// CreateIndex parses and returns a create index data.
func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	if err := p.lexer.EatKeyword("index"); err != nil {
		return nil, err
	}
	idx, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatKeyword("on"); err != nil {
		return nil, err
	}
	table, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim('('); err != nil {
		return nil, err
	}
	field, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.lexer.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewCreateIndexData(idx, table, field), nil
}
