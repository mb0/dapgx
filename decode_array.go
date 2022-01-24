// The text array details were mostly rewritten based on pgtype.
// pgtype uses the MIT LICENSE and the Copyright (c) 2013-2021 Jack Christensen
package dapgx

import (
	"fmt"
	"io"
	"math"
	"unicode/utf8"

	"github.com/jackc/pgtype"
)

// rawTextArray is a rewrite of pgtype.UntypedTextArray that avoids fixed buffer allocations and
// most of the []byte to string copies and instead returns slices from the given raw input where
// possible (that is unless a quoted element contains an escape sequence).
// the usual path was buffer(string(raw)) + n*(bytes(string(buffer(el))))
// with (n+1)*buffer + (n+1)*string(bytes) + n*bytes(string) allocations
// now we only allocated bytes slices for the few quoted elements that contain escape sequences.
type rawTextArray struct {
	Els  []rawTextArrayElement
	Dims []pgtype.ArrayDimension
}

type rawTextArrayElement struct {
	Raw    []byte
	Quoted bool
}

func (el rawTextArrayElement) IsNull() bool {
	r := el.Raw
	return !el.Quoted && len(r) == 4 && r[0] == 'N' && r[1] == 'U' && r[2] == 'L' && r[3] == 'L'
}

func parseRawTextArray(raw []byte) (*rawTextArray, error) {
	dst := &rawTextArray{}
	p := &textArrayParser{raw: raw}
	p.skipWs()

	var explicitDims []pgtype.ArrayDimension

	// Array has explicit dimensions
	for p.peek() == '[' {
		p.readRune()
		lower, err := p.parseInteger()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}

		r, _, _ := p.readRune()
		if r != ':' {
			return nil, fmt.Errorf("invalid array, expected ':' got %v", r)
		}

		upper, err := p.parseInteger()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}

		r, _, _ = p.readRune()
		if r != ']' {
			return nil, fmt.Errorf("invalid array, expected ']' got %v", r)
		}

		explicitDims = append(explicitDims, pgtype.ArrayDimension{
			LowerBound: lower, Length: upper - lower + 1,
		})
	}
	if p.peek() == '=' {
		p.readRune()
	}
	implicitDims := []pgtype.ArrayDimension{{LowerBound: 1, Length: 0}}

	if c := p.peek(); c != '{' {
		return nil, fmt.Errorf("invalid array, expected '{': %v", c)
	}
	// Consume all initial opening brackets. This provides number of dimensions.
	for ok := true; ok; ok = p.peek() == '{' {
		p.readRune()
		implicitDims[len(implicitDims)-1].Length = 1
		implicitDims = append(implicitDims, pgtype.ArrayDimension{LowerBound: 1})
	}
	currentDim := len(implicitDims) - 1
	counterDim := currentDim

	for {
		switch p.peek() {
		case '{':
			p.readRune()
			if currentDim == counterDim {
				implicitDims[currentDim].Length++
			}
			currentDim++
		case ',':
			p.readRune()
		case '}':
			p.readRune()
			currentDim--
			if currentDim < counterDim {
				counterDim = currentDim
			}
		default:
			raw, quoted, err := p.parseValue()
			if err != nil {
				return nil, fmt.Errorf("invalid array value: %v", err)
			}
			if currentDim == counterDim {
				implicitDims[currentDim].Length++
			}
			dst.Els = append(dst.Els, rawTextArrayElement{Raw: raw, Quoted: quoted})
		}

		if currentDim < 0 {
			break
		}
	}

	p.skipWs()

	if len(p.raw)-p.off > 0 {
		return nil, fmt.Errorf("unexpected trailing data: %s", p.raw[p.off:])
	}

	if len(dst.Els) == 0 {
		dst.Dims = nil
	} else if len(explicitDims) > 0 {
		dst.Dims = explicitDims
	} else {
		dst.Dims = implicitDims
	}

	return dst, nil
}

type textArrayParser struct {
	raw []byte
	off int
	n   int
}

func (p *textArrayParser) peek() byte {
	if p.off < len(p.raw) {
		return p.raw[p.off]
	}
	return 0
}

func (p *textArrayParser) readRune() (r rune, n int, _ error) {
	if p.off >= len(p.raw) {
		return 0, 0, io.EOF
	}
	if c := p.raw[p.off]; c < utf8.RuneSelf {
		r, n = rune(c), 1
	} else {
		r, n = utf8.DecodeRune(p.raw[p.off:])
	}
	p.off += n
	p.n = n
	return r, n, nil
}

func (p *textArrayParser) skipWs() {
	for p.off < len(p.raw) {
		if c := p.raw[p.off]; c == ' ' || c > '\t' && c < '\r' {
			p.off++
			p.n = 1
			continue
		}
		break
	}
}

func (p *textArrayParser) parseValue() ([]byte, bool, error) {
	if p.peek() == '"' {
		return p.parseQuotedValue()
	}
	mark := p.off
	for {

		switch p.peek() {
		case ',', '}':
			return p.raw[mark:p.off], false, nil
		}
		_, _, err := p.readRune()
		if err != nil {
			return nil, false, err
		}
	}
}

func (p *textArrayParser) parseQuotedValue() ([]byte, bool, error) {
	p.readRune()
	var res []byte
	mark := p.off
	for {
		switch p.peek() {
		case '\\':
			if res == nil {
				res = append(res, p.raw[mark:p.off-1]...)
			}
			p.readRune()
			_, n, err := p.readRune()
			if err != nil {
				return nil, false, err
			}
			res = append(res, p.raw[p.off-n:p.off]...)
		case '"':
			p.readRune()
			if res == nil {
				res = p.raw[mark : p.off-1]
			}
			return res, true, nil
		default:
			_, n, err := p.readRune()
			if err != nil {
				return nil, false, err
			}
			if res != nil {
				res = append(res, p.raw[p.off-n:p.off]...)
			}
		}
	}
}

func (p *textArrayParser) parseInteger() (int32, error) {
	if c := p.peek(); c < '0' || c > '9' {
		return 0, fmt.Errorf("no number found")
	}
	var res int64
	for p.off < len(p.raw) {
		if c := p.raw[p.off]; c >= '0' && c <= '9' {
			res = res*10 + int64(c-'0')
			if res > math.MaxInt32 {
				return 0, fmt.Errorf("number exceeds max int32")
			}
			p.off++
			p.n = 1
			continue
		}
		break
	}
	return int32(res), nil
}
