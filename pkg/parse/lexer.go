package parse

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

type TokenType int

const (
	Unknown TokenType = iota
	EOF
	Word
	Number
	Other
)

var (
	errBadSyntax = errors.New("parse: bad syntax")
)

var keywords = []string{
	"select",
	"from",
	"where",
	"and",
	"insert",
	"into",
	"values",
	"delete",
	"update",
	"set",
	"create",
	"table",
	"int",
	"varchar",
	"view",
	"as",
	"index",
	"on",
}

// Lexer is the lexical analyzer.
type Lexer struct {
	keywords map[string]bool
	tok      *bufio.Scanner
	typ      rune
	sval     string
	nval     int
}

func ScanSqlChars(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := 0

	for start < len(data) && (data[start] == ' ') {
		start++
	}

	if start >= len(data) {
		return
	}

	if data[start] == '(' || data[start] == ')' || data[start] == ',' || data[start] == '=' {
		return start + 1, data[start : start+1], nil
	}

	// Find the end of the current token
	for i := start; i < len(data); i++ {
		if data[i] == ' ' || data[i] == '(' || data[i] == ')' || data[i] == ',' || data[i] == '=' {
			if data[i] == '(' || data[i] == ')' || data[i] == ',' || data[i] == '=' {
				return i, data[start:i], nil
			}
			return i + 1, data[start:i], nil
		}
	}

	// If we're at the end of the data and there's still some token left
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	return
}

// NewLexer creates a new lexical analyzer for SQL statement s.
func NewLexer(s string) *Lexer {
	l := &Lexer{
		keywords: initKeywords(),
		tok:      bufio.NewScanner(strings.NewReader(s)),
	}
	l.tok.Split(ScanSqlChars)
	l.nextToken()
	return l
}

// matchDelim returns true if the current token is the specified delimiter character.
func (l *Lexer) MatchDelim(d rune) bool {
	// ttype == 'W and sval == d
	// '=' のケースに対応
	// if l.MatchKeyword(string(d)) && len(l.sval) == 1 {
	// 	return rune(l.sval[0]) == d
	// }
	return d == rune(l.sval[0])
}

// matchIntConstant returns true if the current token is an integer.
func (l *Lexer) matchIntConstant() bool {
	return l.typ == 'N' // Assuming 'N' represents a number
}

// matchStringConstant returns true if the current token is a string.
func (l *Lexer) MatchStringConstant() bool {
	// return l.ttype == 'S' // Assuming 'S' represents a string
	return rune(l.sval[0]) == '\''
}

// matchKeyword returns true if the current token is the specified keyword.
func (l *Lexer) MatchKeyword(w string) bool {
	return l.typ == 'W' && l.sval == w // Assuming 'W' represents a word
}

// matchId returns true if the current token is a legal identifier.
func (l *Lexer) MatchId() bool {
	return l.typ == 'W' && !l.keywords[l.sval]
}

// eatDelim throws an exception if the current token is not the specified delimiter. Otherwise, moves to the next token.
func (l *Lexer) EatDelim(d rune) error {
	if !l.MatchDelim(d) {
		return errBadSyntax
	}
	l.nextToken()
	return nil
}

// eatIntConstant throws an exception if the current token is not an integer. Otherwise, returns that integer and moves to the next token.
func (l *Lexer) EatIntConstant() (int, error) {
	if !l.matchIntConstant() {
		return 0, errBadSyntax
	}
	i := l.nval
	l.nextToken()
	return i, nil
}

// eatStringConstant throws an exception if the current token is not a string. Otherwise, returns that string and moves to the next token.
func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", errBadSyntax
	}
	s := l.sval
	l.nextToken()
	return s, nil
}

// eatKeyword throws an exception if the current token is not the specified keyword. Otherwise, moves to the next token.
func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return errBadSyntax
	}
	l.nextToken()
	return nil
}

// eatId throws an exception if the current token is not an identifier. Otherwise, returns the identifier string and moves to the next token.
func (l *Lexer) EatId() (string, error) {
	if !l.MatchId() {
		return "", errBadSyntax
	}
	s := l.sval
	l.nextToken()
	return s, nil
}

func (l *Lexer) nextToken() {
	if l.tok.Scan() {
		// Here, we're making a simple assumption about token types. You might need to adjust this based on your actual needs.
		token := l.tok.Text()
		if _, err := strconv.Atoi(token); err == nil {
			l.typ = 'N'
			l.nval, _ = strconv.Atoi(token)
			return
		}
		if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") {
			l.typ = 'S'
			l.sval = token
			// l.sval = token[1 : len(token)-1]
			return
		}
		l.typ = 'W'
		l.sval = strings.ToLower(token)
		return
	}
	l.typ = -1 // FIXME
	l.typ = '.'
}

func initKeywords() map[string]bool {
	m := make(map[string]bool)
	for _, k := range keywords {
		m[k] = true
	}
	return m
}
