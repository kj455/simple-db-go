package parse

import "fmt"

type PredParser struct {
	lexer *Lexer
}

func NewPredParser(input string) *PredParser {
	return &PredParser{
		lexer: NewLexer(input),
	}
}

// Field parses and returns a field name (identifier).
func (p *PredParser) Field() (string, error) {
	field, err := p.lexer.EatId()
	if err != nil {
		return "", fmt.Errorf("expected field name: %w", err)
	}
	return field, nil
}

// Constant parses either an integer or a string constant.
func (p *PredParser) Constant() error {
	if p.lexer.MatchStringConstant() {
		if _, err := p.lexer.EatStringConstant(); err != nil {
			return fmt.Errorf("expected string constant: %w", err)
		}
		return nil
	}
	if _, err := p.lexer.EatIntConstant(); err != nil {
		return fmt.Errorf("expected integer constant: %w", err)
	}
	return nil
}

// Expression parses a field or constant.
func (p *PredParser) Expression() error {
	if p.lexer.MatchId() {
		if _, err := p.Field(); err != nil {
			return err
		}
	} else if err := p.Constant(); err != nil {
		return err
	}
	return nil
}

// Term parses an equality condition of the form `expression = expression`.
func (p *PredParser) Term() error {
	if err := p.Expression(); err != nil {
		return fmt.Errorf("invalid term: %w", err)
	}
	if err := p.lexer.EatDelim('='); err != nil {
		return fmt.Errorf("expected '=' delimiter: %w", err)
	}
	if err := p.Expression(); err != nil {
		return fmt.Errorf("invalid term after '=': %w", err)
	}
	return nil
}

// Predicate parses a logical predicate with optional "AND" chaining.
func (p *PredParser) Predicate() error {
	if err := p.Term(); err != nil {
		return err
	}
	for p.lexer.MatchKeyword("and") {
		if err := p.lexer.EatKeyword("and"); err != nil {
			return fmt.Errorf("expected 'AND' keyword: %w", err)
		}
		if err := p.Predicate(); err != nil {
			return err
		}
	}
	return nil
}
