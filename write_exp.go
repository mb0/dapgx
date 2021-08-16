package dapgx

import (
	"fmt"
	"strings"

	"xelf.org/xelf/bfr"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/typ"
)

// WriteExp writes the element e to w or returns an error.
// This is used for explicit selectors for example.
func WriteExp(w *Writer, env exp.Env, e exp.Exp) error {
	switch v := e.(type) {
	case *exp.Sym:
		n, l, err := w.Translate(w.Prog, env, v)
		if err != nil {
			return fmt.Errorf("symbol %q: %w", v.Sym, err)
		}
		if l != nil {
			return WriteVal(w, l.Type(), l)
		}
		return writeIdent(w, n)
	case *exp.Call:
		return WriteCall(w, env, v)
	case *exp.Lit:
		return WriteLit(w, v)
	}
	return fmt.Errorf("unexpected element %[1]T %[1]s", e)
}

// WriteCall writes the expression e to w using env or returns an error.
// Most xelf expressions with resolvers from the core or lib built-ins have a corresponding
// expression in postgresql. Custom resolvers can be rendered to sql by detecting
// and handling them before calling this function.
func WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	key := cor.Keyed(exp.SigName(e.Sig))
	r := exprWriterMap[key]
	if r != nil {
		return r.WriteCall(w, env, e)
	}
	// dyn and reduce are not supported
	// TODO let and with might use common table expressions on a higher level
	return fmt.Errorf("no writer for expression %s", e)
}

type callWriter interface {
	WriteCall(*Writer, exp.Env, *exp.Call) error
}

var exprWriterMap map[string]callWriter

func init() {
	// TODO update outdated and incomplete call wrtiers
	exprWriterMap = map[string]callWriter{
		// I found no better way sql expression to fail when resolved but not otherwise.
		// Sadly we cannot transport any failure message, but it suffices, because this is
		// only meant to be a test helper.
		"fail":  writeRaw{".321/0", PrecCmp}, // 3..2..1..boom!
		"if":    writeFunc(renderIf),
		"and":   writeLogic{" AND ", false, PrecAnd},
		"or":    writeLogic{" OR ", false, PrecOr},
		"ok":    writeLogic{" AND ", false, PrecAnd},
		"not":   writeLogic{" AND ", true, PrecAnd},
		"add":   writeArith{" + ", PrecAdd},
		"sub":   writeArith{" - ", PrecAdd},
		"mul":   writeArith{" * ", PrecMul},
		"div":   writeArith{" / ", PrecMul},
		"eq":    writeEq{" = ", false},
		"ne":    writeEq{" != ", false},
		"equal": writeEq{" = ", true},
		"lt":    writeCmp(" < "),
		"gt":    writeCmp(" > "),
		"le":    writeCmp(" <= "),
		"ge":    writeCmp(" >= "),
		"con":   writeFunc(writeCon),
		"cat":   writeFunc(writeCat),
	}
}

const (
	_ = iota
	PrecOr
	PrecAnd
	PrecNot
	PrecIs  // , is null, is not null, …
	PrecCmp // <, >, =, <=, >=, <>, !=
	PrecIn  // , between, like, ilike, similar
	PrecDef
	PrecAdd // +, -
	PrecMul // *, /, %
)

type (
	writeRaw struct {
		raw  string
		prec int
	}
	writeFunc  func(*Writer, exp.Env, *exp.Call) error
	writeArith struct {
		op   string
		prec int
	}
	writeCmp   string
	writeLogic struct {
		op   string
		not  bool
		prec int
	}
	writeEq struct {
		op     string
		strict bool
	}
)

func (r writeRaw) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	restore := w.Prec(r.prec)
	w.WriteString(r.raw)
	restore()
	return nil
}
func (r writeFunc) WriteCall(w *Writer, env exp.Env, e *exp.Call) error { return r(w, env, e) }
func (r writeLogic) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(r.prec)()
	var i int
	return each(e.Args, func(a exp.Exp) error {
		if i++; i > 1 {
			w.WriteString(r.op)
		}
		return writeBool(w, env, r.not, a)
	})
}

func (r writeArith) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(r.prec)()
	var i int
	return each(e.Args, func(a exp.Exp) error {
		if i++; i > 1 {
			w.WriteString(r.op)
		}
		return WriteExp(w, env, a)
	})
}

func renderIf(w *Writer, env exp.Env, e *exp.Call) error {
	restore := w.Prec(PrecDef)
	w.WriteString("CASE ")
	cases := e.Args[0].(*exp.Tupl).Els
	for _, c := range cases {
		tupl := c.(*exp.Tupl).Els
		w.WriteString("WHEN ")
		err := writeBool(w, env, false, tupl[0])
		if err != nil {
			return err
		}
		w.WriteString(" THEN ")
		err = WriteExp(w, env, tupl[1])
		if err != nil {
			return err
		}
	}
	els := e.Args[1]
	if els != nil {
		w.WriteString(" ELSE ")
		err := WriteExp(w, env, els)
		if err != nil {
			return err
		}
	}
	w.WriteString(" END")
	restore()
	return nil
}

func (r writeEq) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	if len(e.Args) > 2 {
		defer w.Prec(PrecAnd)()
	}
	// TODO mind nulls
	fst, err := writeString(w, env, e.Args[0])
	if err != nil {
		return err
	}
	for i, arg := range e.Args[1].(*exp.Tupl).Els {
		if i > 0 {
			w.WriteString(" AND ")
		}
		if !r.strict {
			restore := w.Prec(PrecCmp)
			w.WriteString(fst)
			w.WriteString(r.op)
			err = WriteExp(w, env, arg)
			if err != nil {
				return err
			}
			restore()
			continue
		}
		oth, err := writeString(w, env, arg)
		if err != nil {
			return err
		}
		w.Fmt("(%[1]s%[2]s%[3]s AND pg_typeof(%[1]s)%[2]spg_typeof(%[3]s))", fst, r.op, oth)
	}
	return nil
}

func (r writeCmp) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	if len(e.Args) > 2 {
		defer w.Prec(PrecAnd)()
	}
	// TODO mind nulls
	last, err := writeString(w, env, e.Args[0])
	if err != nil {
		return err
	}
	for i, arg := range e.Args[1].(*exp.Tupl).Els {
		if i > 0 {
			w.WriteString(" AND ")
		}
		restore := w.Prec(PrecCmp)
		w.WriteString(last)
		w.WriteString(string(r))
		oth, err := writeString(w, env, arg)
		if err != nil {
			return err
		}
		w.WriteString(oth)
		restore()
		last = oth
	}
	return nil
}

func writeCon(w *Writer, env exp.Env, e *exp.Call) error {
	if len(e.Args) < 1 {
		return fmt.Errorf("empty con expression")
	}
	t, ok := e.Args[0].(*exp.Lit).Val.(typ.Type)
	if !ok {
		return fmt.Errorf("con expression must start with a type")
	}
	ts, err := TypString(t)
	if err != nil {
		return err
	}
	if len(e.Args) < 2 {
		zero, _, err := zeroStrings(t)
		if err != nil {
			return err
		}
		w.WriteString(zero)
	} else {
		tup, ok := e.Args[1].(*exp.Tupl)
		if !ok {
			return fmt.Errorf("con expression start with a type")
		}
		if len(tup.Els) != 1 {
			return fmt.Errorf("not implemented %q", e)
		}
		if a, ok := tup.Els[0].(*exp.Lit); ok {
			el := &exp.Lit{Val: a.Val, Src: a.Src}
			err = WriteExp(w, env, el)
			if err != nil {
				return err
			}
		}
	}
	w.WriteString("::")
	w.WriteString(ts)
	return nil
}

func writeCat(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(PrecDef)()
	var i int
	return each(e.Args, func(a exp.Exp) error {
		if i++; i > 1 {
			w.WriteString(" || ")
		}
		return WriteExp(w, env, a)
	})
}

func each(args []exp.Exp, f func(exp.Exp) error) error {
	for _, arg := range args {
		switch d := arg.(type) {
		case *exp.Tupl:
			for _, a := range d.Els {
				err := f(a)
				if err != nil {
					return err
				}
			}
		default:
			err := f(arg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeBool(w *Writer, env exp.Env, not bool, e exp.Exp) error {
	t := e.Resl()
	if t.Kind == knd.Bool {
		if not {
			defer w.Prec(PrecNot)()
			w.WriteString("NOT ")
		}
		return WriteExp(w, env, e)
	}
	// add boolean conversion if necessary
	if t.Kind&knd.None != 0 {
		defer w.Prec(PrecIs)()
		err := WriteExp(w, env, e)
		if err != nil {
			return err
		}
		if not {
			w.WriteString(" IS NULL")
		} else {
			w.WriteString(" IS NOT NULL")
		}
		return nil
	}
	cmp, oth, err := zeroStrings(t)
	if err != nil {
		return err
	}
	if oth != "" {
		if not {
			defer w.Prec(PrecOr)()
		} else {
			defer w.Prec(PrecAnd)()
		}
	} else if cmp != "" {
		defer w.Prec(PrecCmp)()
	}
	err = WriteExp(w, env, e)
	if err != nil {
		return err
	}
	if cmp != "" {
		op := " != "
		if not {
			op = " = "
		}
		restore := w.Prec(PrecCmp)
		w.WriteString(op)
		w.WriteString(cmp)
		if oth != "" {
			if not {
				w.WriteString(" OR ")
			} else {
				w.WriteString(" AND ")
			}
			err := WriteExp(w, env, e)
			if err != nil {
				return err
			}
			w.WriteString(op)
			w.WriteString(oth)
		}
		restore()
	}
	return nil
}

func writeString(w *Writer, env exp.Env, e exp.Exp) (string, error) {
	cc := *w
	var b strings.Builder
	cc.P = bfr.P{Writer: &b}
	err := WriteExp(&cc, env, e)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func elType(e exp.Exp) typ.Type {
	return e.Resl()
}

func zeroStrings(t typ.Type) (zero, alt string, _ error) {
	switch t.Kind & knd.Prim {
	case knd.Bool:
	case knd.Num, knd.Int, knd.Real, knd.Bits:
		zero = "0"
	case knd.Char, knd.Str, knd.Raw:
		zero = "''"
	case knd.Span:
		zero = "'0'"
	case knd.Time:
		zero = "'0001-01-01Z'"
	case knd.Enum:
		// TODO
	case knd.List:
		// TODO check if postgres array otherwise
		fallthrough
	case knd.Idxr:
		zero, alt = "'null'", "'[]'"
	case knd.Keyr, knd.Dict, knd.Rec, knd.Obj:
		zero, alt = "'null'", "'{}'"
	default:
		return "", "", fmt.Errorf("error unexpected type %s", t)
	}
	return
}

// TODO collect keywords
var keywords map[string]struct{}

func writeIdent(w *Writer, name string) error {
	name = strings.ToLower(name)
	if _, ok := keywords[name]; !ok {
		return w.Fmt(name)
	}
	w.WriteByte('"')
	w.WriteString(name)
	return w.WriteByte('"')
}
