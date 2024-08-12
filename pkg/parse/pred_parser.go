package parse

import "fmt"

type PredParser struct {
	lex *Lexer
}

func NewPredParser(s string) *PredParser {
	return &PredParser{lex: NewLexer(s)}
}

func (p *PredParser) Field() (string, error) {
	return p.lex.EatId()
}

func (p *PredParser) Constant() error {
	if p.lex.MatchStringConstant() {
		_, err := p.lex.EatStringConstant()
		return err
	}
	_, err := p.lex.EatIntConstant()
	return err
}

func (p *PredParser) Expression() error {
	if p.lex.MatchId() {
		_, err := p.Field()
		return err
	}
	return p.Constant()
}

func (p *PredParser) Term() error {
	if err := p.Expression(); err != nil {
		return err
	}
	if err := p.lex.EatDelim('='); err != nil {
		return err
	}
	return p.Expression()
}

func (p *PredParser) Predicate() error {
	if err := p.Term(); err != nil {
		return err
	}
	if p.lex.MatchKeyword("and") {
		if err := p.lex.EatKeyword("and"); err != nil {
			return fmt.Errorf("parse: %w", err)
		}
		if err := p.Predicate(); err != nil {
			return err
		}
	}
	return nil
}
