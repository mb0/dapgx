package dapgx

import (
	"fmt"
	"strings"

	"xelf.org/xelf/bfr"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

func TypString(t typ.Type) (string, error) {
	switch t.Kind & knd.Any {
	case knd.Bool:
		return "bool", nil
	case knd.Int, knd.Bits:
		return "int8", nil
	case knd.Num, knd.Real:
		return "float8", nil
	case knd.Char, knd.Str:
		return "text", nil
	case knd.Enum:
		return strings.ToLower(t.Ref), nil
	case knd.Raw:
		return "bytea", nil
	case knd.UUID:
		return "uuid", nil
	case knd.Time:
		return "timestamptz", nil
	case knd.Span:
		return "interval", nil
	case knd.Any, knd.All, knd.Data, knd.Keyr, knd.Dict, knd.Obj:
		return "jsonb", nil
	case knd.List, knd.Idxr:
		if n := typ.ContEl(t); n.Kind&knd.Prim == n.Kind {
			res, err := TypString(n)
			if err != nil {
				return "", err
			}
			return res + "[]", nil
		}
		return "jsonb", nil
	}
	return "", fmt.Errorf("unexpected type %s", t)
}

// WriteLit renders the literal l to b or returns an error.
func WriteLit(b *Writer, l *exp.Lit) error { return WriteVal(b, typ.Res(l.Type()), l.Val) }

func WriteVal(b *Writer, t typ.Type, l lit.Val) error {
	if l.Nil() {
		return b.Fmt("NULL")
	}
	l = lit.Unwrap(l)
	if t == typ.Void {
		t = l.Type()
	}
	switch k := t.Kind & knd.Data; true {
	case k == knd.Data:
		return writeJSONB(b, l)
	case k == knd.Bool:
		if l.Zero() {
			return b.Fmt("FALSE")
		}
		return b.Fmt("TRUE")
	case k&knd.Num != 0:
		return l.Print(&b.P)
	case k == knd.Raw:
		return writeSuffix(b, l, "::bytea")
	case k == knd.UUID:
		return writeSuffix(b, l, "::uuid")
	case k == knd.Time:
		return writeSuffix(b, l, "::timestamptz")
	case k == knd.Span:
		return writeSuffix(b, l, "::interval")
	case k&knd.Char != 0:
		return WriteQuote(b, l.String())
	case k == knd.List:
		if e := typ.El(t); e.Kind != knd.Void && e.Kind&knd.Prim == e.Kind&knd.Any {
			// use postgres array for one dimensional primitive arrays
			return writeArray(b, l.(lit.Idxr))
		}
		return writeJSONB(b, l) // otherwise use jsonb
	case k&knd.Keyr != 0:
		return writeJSONB(b, l)
	}
	return fmt.Errorf("unexpected lit %s %s", t, l)
}

// WriteQuote quotes a string as a postgres string, all single quotes are use sql escaping.
func WriteQuote(w *Writer, text string) error { return w.Fmt(Quote(text)) }
func Quote(text string) string {
	return fmt.Sprintf("'%s'", strings.Replace(text, "'", "''", -1))
}

func writeSuffix(w *Writer, l lit.Val, fix string) error {
	err := l.Print(&w.P)
	if err != nil {
		return err
	}
	return w.Fmt(fix)
}

func writeJSONB(w *Writer, l lit.Val) error {
	var bb strings.Builder
	err := l.Print(&bfr.P{Writer: &bb, JSON: true})
	if err != nil {
		return err
	}
	WriteQuote(w, bb.String())
	return w.Fmt("::jsonb")
}

func writeArray(w *Writer, l lit.Idxr) error {
	var bb strings.Builder
	bb.WriteByte('{')
	err := l.IterIdx(func(i int, e lit.Val) error {
		if i > 0 {
			bb.WriteByte(',')
		}
		return e.Print(&bfr.P{Writer: &bb, JSON: true})
	})
	if err != nil {
		return err
	}
	bb.WriteByte('}')
	WriteQuote(w, bb.String())
	t, err := TypString(typ.ContEl(l.Type()))
	if err != nil {
		return err
	}
	return w.Fmt("::%s[]", t)
}
