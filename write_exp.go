package dapgx

import (
	"fmt"
	"sort"
	"strings"

	"xelf.org/xelf/bfr"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
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
		return WriteIdent(w, n)
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
	key := cor.Keyed(e.Sig.Ref)
	r := exprWriterMap[key]
	if r != nil {
		return r.WriteCall(w, env, e)
	}
	// dyn and reduce are not supported
	// TODO let and with might use common table expressions on a higher level
	return fmt.Errorf("no writer for expression %s %s", key, e)
}

type callWriter interface {
	WriteCall(*Writer, exp.Env, *exp.Call) error
}

var exprWriterMap map[string]callWriter

func init() {
	// TODO think about std specs dot let mut append fold as well as extlib specs
	exprWriterMap = map[string]callWriter{
		"or":  writeLogic{" OR ", false, PrecOr},
		"and": writeLogic{" AND ", false, PrecAnd},
		"ok":  writeLogic{" AND ", false, PrecAnd},
		"not": writeLogic{" AND ", true, PrecAnd},
		// I found no better way for sql expression to fail when resolved but not otherwise.
		// Sadly we cannot transport any failure message, but it suffices, because this is
		// only meant to be a test helper.
		"err":   writeRaw{".321/0", PrecCmp}, // 3..2..1..boom!
		"add":   writeArith{" + ", PrecAdd},
		"sub":   writeArith{" - ", PrecAdd},
		"mul":   writeArith{" * ", PrecMul},
		"div":   writeArith{" / ", PrecMul},
		"rem":   writeArith{" % ", PrecMul},
		"abs":   writeFunc(renderCall("ABS")),
		"neg":   writeFunc(renderNeg),
		"min":   writeFunc(renderCall("LEAST")),
		"max":   writeFunc(renderCall("GREATEST")),
		"eq":    writeEq{" = ", false},
		"ne":    writeEq{" != ", false},
		"lt":    writeCmp{" < "},
		"ge":    writeCmp{" >= "},
		"gt":    writeCmp{" > "},
		"le":    writeCmp{" <= "},
		"in":    writeIn{false},
		"ni":    writeIn{true},
		"equal": writeEq{" = ", true},
		"if":    writeFunc(renderIf),
		"swt":   writeFunc(renderSwt),
		"df":    writeFunc(renderCall("COALESCE")),
		"cat":   writeFunc(renderCall("CONCAT")),
		"sep":   writeFunc(renderSep),
		"xelf":  writeFunc(renderJSON), // json is valid xelf that postgres understands
		"json":  writeFunc(renderJSON),
		"make":  writeFunc(renderMake),
		"len":   writeFunc(renderLen),
		// dyn:      should already be resolved. lazily resolved dyns are disallowed.
		// dot, let: we should be able to replace all occurrences of the declarations
		//           otherwise we can use with ctes
		// append:   for typed and jsonb arrays
		// mut:      for typed and jsonb arrays, json object
		// fn, fold, foldr, range: maybe possible as inline subquery?
		"index": writeFunc(renderCall("strpos")),
		// "last": (length($1) - strpos($1, $2))
		"prefix":   writeLike{dir: 1}, // $1 like $2||'%'
		"suffix":   writeLike{dir: 2}, // $1 like '%'||$2
		"contains": writeLike{dir: 3}, // $1 like '%'||$2||'%'
		"upper":    writeFunc(renderCall("upper")),
		"lower":    writeFunc(renderCall("lower")),
		"trim":     writeFunc(renderCallOpt("trim", "both ' \t' from ")),
		"like":     writeLike{},          // $1 like $2
		"ilike":    writeLike{ign: true}, // $1 ilike $2
	}
}

type writePlain struct {
	pre string
	btw string
	suf string
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
	writeCmp struct {
		op string
	}
	writeIn struct {
		not bool
	}
	writeLogic struct {
		op   string
		not  bool
		prec int
	}
	writeEq struct {
		op     string
		strict bool
	}
	writeLike struct {
		ign bool
		dir byte
	}
)

func (r writePlain) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(PrecIn)()
	w.Fmt(r.pre)
	var i int
	each(e.Args, func(a exp.Exp) error {
		if i++; i > 1 {
			w.Fmt(r.btw)
		}
		return WriteExp(w, env, a)
	})
	return w.Fmt(r.suf)
}
func (r writeLike) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(PrecIn)()
	var i int
	each(e.Args, func(a exp.Exp) error {
		switch i {
		case 1:
			if r.ign {
				w.WriteString(" ilike ")
			} else {
				w.WriteString(" like ")
			}
			if r.dir&2 != 0 {
				w.WriteString("'%'||")
			}
			if r.dir != 0 {
				w.WriteString("replace(replace(replace(")
				err := WriteExp(w, env, a)
				w.WriteString(`, '\', '\\'), '_', '\_'), '%', '\%')`)
				return err
			}
			return WriteExp(w, env, a)
		}
		i++
		return WriteExp(w, env, a)
	})
	if r.dir&1 != 0 {
		w.WriteString("||'%'")
	}
	return nil
}
func (r writeRaw) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(r.prec)()
	return w.Fmt(r.raw)
}
func (r writeFunc) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	return r(w, env, e)
}
func (r writeLogic) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(r.prec)()
	var i int
	return each(e.Args, func(a exp.Exp) error {
		if i++; i > 1 {
			w.Fmt(r.op)
		}
		return writeBool(w, env, r.not, a)
	})
}
func renderNeg(w *Writer, env exp.Env, e *exp.Call) error {
	num, ok := e.Args[0].(*exp.Lit)
	if ok {
		str := num.String()
		if str[0] == '-' {
			str = str[1:]
		} else {
			w.Byte('-')
		}
		return w.Fmt(str)
	}
	w.Byte('-')
	return WriteExp(w, env, e.Args[0])
}
func renderLen(w *Writer, env exp.Env, e *exp.Call) error {
	fst := e.Args[0]
	if l, ok := fst.(*exp.Lit); ok {
		var n int
		lenr, ok := lit.Unwrap(l.Val).(lit.Lenr)
		if ok && lenr != nil {
			n = lenr.Len()
		}
		return w.Fmt("%d", n)
	}
	str, err := writeString(w, env, fst)
	if err != nil {
		return err
	}
	t := typ.Res(fst.Type())
	switch {
	case t.Kind&knd.Char != 0:
		return w.Fmt("octet_length(%s)", str)
	case t.Kind&knd.Keyr != 0:
		return w.Fmt("(SELECT COUNT(*) FROM jsonb_object_keys(%s))", str)
	case t.Kind&knd.Idxr != 0:
		el := typ.ContEl(t)
		if el.Kind&knd.Data == knd.Data {
			return w.Fmt("jsonb_array_length(%s)", str)
		}
		return w.Fmt("array_length(%s, 1)", str)
	}
	return fmt.Errorf("len of unexpected type %s", t)
}

func (r writeArith) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(r.prec)()
	return writeEach(w, env, e.Args, r.op)
}

func renderCall(name string) writeFunc {
	return renderCallOpt(name, "")
}
func renderCallOpt(name, pre string) writeFunc {
	return func(w *Writer, env exp.Env, e *exp.Call) error {
		defer w.Prec(PrecDef)()
		w.Fmt(name)
		w.Byte('(')
		w.Fmt(pre)
		err := writeEach(w, env, e.Args, ", ")
		if err != nil {
			return err
		}
		return w.Byte(')')
	}
}

func renderIf(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(PrecOr)()
	w.Fmt("CASE")
	cases := e.Args[0].(*exp.Tupl).Els
	for i := 0; i < len(cases); i += 2 {
		w.Fmt(" WHEN ")
		err := writeBool(w, env, false, cases[i])
		if err != nil {
			return err
		}
		w.Fmt(" THEN ")
		err = WriteExp(w, env, cases[i+1])
		if err != nil {
			return err
		}
	}
	els := e.Args[1]
	if els != nil {
		w.Fmt(" ELSE ")
		err := WriteExp(w, env, els)
		if err != nil {
			return err
		}
	}
	return w.Fmt(" END")
}

func renderSwt(w *Writer, env exp.Env, e *exp.Call) error {
	defer w.Prec(PrecDef)()
	fst, err := writeString(w, env, e.Args[0])
	if err != nil {
		return err
	}
	w.Fmt("CASE")
	cases := e.Args[1].(*exp.Tupl).Els
	for i := 0; i < len(cases); i += 2 {
		w.Fmt(" WHEN %s = ", fst)
		err = WriteExp(w, env, cases[i])
		if err != nil {
			return err
		}
		w.Fmt(" THEN ")
		err = WriteExp(w, env, cases[i+1])
		if err != nil {
			return err
		}
	}
	els := e.Args[2]
	if els != nil {
		w.Fmt(" ELSE ")
		err := WriteExp(w, env, els)
		if err != nil {
			return err
		}
	}
	return w.Fmt(" END")
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
			w.Fmt(" AND ")
		}
		if !r.strict {
			restore := w.Prec(PrecCmp)
			w.Fmt(fst)
			w.Fmt(r.op)
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
			w.Fmt(" AND ")
		}
		restore := w.Prec(PrecCmp)
		w.Fmt(last)
		w.Fmt(r.op)
		oth, err := writeString(w, env, arg)
		restore()
		if err != nil {
			return err
		}
		w.Fmt(oth)
		last = oth
	}
	return nil
}
func (r writeIn) WriteCall(w *Writer, env exp.Env, e *exp.Call) error {
	// TODO mind nulls
	last, err := writeString(w, env, e.Args[0])
	if err != nil {
		return err
	}
	args := e.Args[1].(*exp.Tupl).Els
	if len(args) > 1 {
		defer w.Prec(PrecAnd)()
	}
	for i, arg := range e.Args[1].(*exp.Tupl).Els {
		restore := w.Prec(PrecIn)
		if i > 0 {
			if r.not {
				w.Fmt(" AND ")
			} else {
				w.Fmt(" OR ")
			}
		}
		w.Fmt(last)
		if list, ok := arg.(*exp.Lit); ok {
			if r.not {
				w.Fmt(" NOT IN (")
			} else {
				w.Fmt(" IN (")
			}
			idxr, ok := list.Val.(lit.Idxr)
			if !ok {
				return fmt.Errorf("expect idxr got %T", list.Val)
			}
			err = idxr.IterIdx(func(i int, e lit.Val) error {
				if i > 0 {
					w.Fmt(", ")
				}
				return e.Print(&bfr.P{Writer: w.P.Writer, JSON: true})
			})
			if err != nil {
				return err
			}
		} else {
			if r.not {
				w.Fmt(" != ALL(")
			} else {
				w.Fmt(" = ANY(")
			}
			err = WriteExp(w, env, arg)
		}
		restore()
		w.Byte(')')
		if err != nil {
			return err
		}
	}
	return nil
}

func renderMake(w *Writer, env exp.Env, e *exp.Call) error {
	if len(e.Args) < 1 {
		return fmt.Errorf("empty make expression")
	}
	lt, ok := e.Args[0].(*exp.Lit)
	if !ok {
		return fmt.Errorf("make expression must start with a type")
	}
	t, err := typ.ToType(lt.Val)
	if err != nil {
		return err
	}
	tup, ok := e.Args[1].(*exp.Tupl)
	if !ok || len(tup.Els) == 0 {
		zero, _, err := zeroStrings(t)
		if err != nil {
			return err
		}
		w.Fmt(zero)
	} else {
		if len(tup.Els) == 1 {
			if a, ok := tup.Els[0].(*exp.Lit); ok {
				return WriteVal(w, t, a.Val)
			}
			err := WriteExp(w, env, tup.Els[0])
			if err != nil {
				return err
			}
		} else {
			vs := make([]lit.Val, 0, len(tup.Els))
			for _, el := range tup.Els {
				vs = append(vs, el.(*exp.Lit).Val)
			}
			return writeArray(w, lit.NewList(typ.ContEl(t), vs...))
		}
	}
	ts, err := TypString(t)
	if err != nil {
		return err
	}
	return w.Fmt("::%s", ts)
}

func renderSep(w *Writer, env exp.Env, e *exp.Call) (err error) {
	sep := ", "
	l, ok := e.Args[0].(*exp.Lit)
	if ok {
		str, err := lit.ToStr(l.Val)
		if err != nil {
			return err
		}
		sep = fmt.Sprintf(", %s, ", Quote(string(str)))
	} else {
		str, err := writeString(w, env, e.Args[0])
		if err != nil {
			return err
		}
		sep = fmt.Sprintf(", %s, ", str)
	}
	w.Fmt("CONCAT(")
	err = writeEach(w, env, e.Args[1:], sep)
	if err != nil {
		return err
	}
	return w.Byte(')')
}

func renderJSON(w *Writer, env exp.Env, e *exp.Call) (err error) {
	l, ok := e.Args[0].(*exp.Lit)
	if ok {
		var b strings.Builder
		err = l.Print(&bfr.P{Writer: &b, JSON: true})
		if err != nil {
			return err
		}
		err = WriteQuote(w, b.String())
	} else {
		err = WriteExp(w, env, e.Args[0])
	}
	if err != nil {
		return err
	}
	return w.Fmt("::jsonb")
}

func writeBool(w *Writer, env exp.Env, not bool, e exp.Exp) error {
	t := typ.Res(e.Type())
	if t.Kind == knd.Bool {
		if not {
			defer w.Prec(PrecNot)()
			w.Fmt("NOT ")
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
			return w.Fmt(" IS NULL")
		}
		return w.Fmt(" IS NOT NULL")
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
		w.Fmt(op)
		w.Fmt(cmp)
		if oth != "" {
			if not {
				w.Fmt(" OR ")
			} else {
				w.Fmt(" AND ")
			}
			err := WriteExp(w, env, e)
			if err != nil {
				return err
			}
			w.Fmt(op)
			w.Fmt(oth)
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

func writeEach(w *Writer, env exp.Env, args []exp.Exp, sep string) error {
	var i int
	return each(args, func(a exp.Exp) error {
		if i++; i > 1 {
			w.Fmt(sep)
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

func zeroStrings(t typ.Type) (zero, alt string, _ error) {
	switch t.Kind & knd.Prim {
	case knd.Bool:
	case knd.Num, knd.Int, knd.Real, knd.Bits:
		zero = "0"
	case knd.Char, knd.Str, knd.Raw, knd.Enum:
		// TODO find enum default value?
		zero = "''"
	case knd.Span:
		zero = "'0'"
	case knd.Time:
		zero = "'0001-01-01Z'"
	case knd.List:
		// TODO check if postgres array otherwise
		fallthrough
	case knd.Idxr:
		zero, alt = "'null'", "'[]'"
	case knd.Keyr, knd.Dict, knd.Obj:
		zero, alt = "'null'", "'{}'"
	default:
		return "", "", fmt.Errorf("error unexpected type %s", t)
	}
	return
}

func WriteIdent(w bfr.Writer, name string) (err error) {
	if name, ok := Unreserved(name); ok {
		_, err = w.WriteString(name)
	} else {
		w.WriteByte('"')
		w.WriteString(name)
		err = w.WriteByte('"')
	}
	return err
}

// Unreserved returns the lowercase key and whether it is an unreserved identifier.
// If unreserved returns false the key must be escaped with double quotes.
func Unreserved(name string) (string, bool) {
	name = strings.ToLower(name)
	idx := sort.SearchStrings(keys, name)
	return name, idx < 0 || idx >= len(keys) || keys[idx] != name
}

var keys = []string{
	"all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both",
	"case", "cast", "check", "collate", "column", "constraint", "create", "current_catalog",
	"current_date", "current_role", "current_time", "current_timestamp", "current_user",
	"default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false",
	"fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially",
	"intersect", "into", "lateral", "leading", "limit", "localtime", "localtimestamp", "not",
	"null", "offset", "on", "only", "or", "order", "placing", "primary", "references",
	"returning", "select", "session_user", "some", "symmetric", "table", "then", "to",
	"trailing", "true", "union", "unique", "user", "using", "variadic", "when", "where",
	"window", "with",
}
