package dompgx

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"xelf.org/dapgx"
	"xelf.org/daql/dom"
	"xelf.org/xelf/bfr"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/typ"
)

func WriteSchemaFile(fname string, p *dom.Project, s *dom.Schema) error {
	b := bfr.Get()
	defer bfr.Put(b)
	w := dapgx.NewWriter(b, p, nil, nil)
	w.Project = p
	w.Fmt(w.Header)
	w.Fmt("BEGIN;\n\n")
	err := WriteSchema(w, s)
	if err != nil {
		return fmt.Errorf("render file %s error: %v", fname, err)
	}
	w.Fmt("COMMIT;\n")
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, b)
	return err
}

func WriteSchema(w *dapgx.Writer, s *dom.Schema) (err error) {
	// collect models first, we do not want to generate empty schemas
	ms := make([]*dom.Model, 0, len(s.Models))
	for _, m := range s.Models {
		switch m.Kind.Kind {
		case knd.Enum:
			ms = append(ms, m)
		case knd.Obj:
			if hasFlag(m.Extra, "backup") || hasFlag(m.Extra, "topic") {
				ms = append(ms, m)
			}
		}
	}
	if len(ms) == 0 {
		w.Fmt("-- schema %s has no enums or tables\n\n", s.Name)
		return nil
	}
	w.Fmt("CREATE SCHEMA %s;\n\n", warnIdent(s.Name))
	for _, m := range ms {
		err = WriteModel(w, m)
		if err != nil {
			return err
		}
		w.Fmt("\n\n")
	}
	return nil
}

func WriteModel(w *dapgx.Writer, m *dom.Model) error {
	switch m.Kind.Kind {
	case knd.Enum:
		return WriteEnum(w, m)
	default:
		return WriteTable(w, m)
	}
}

func WriteEnum(w *dapgx.Writer, m *dom.Model) error {
	w.Fmt("CREATE TYPE %s.%s AS ENUM (", checkIdent(m.Schema), warnIdent(m.Key()))
	w.Indent()
	w.Fmt("''")
	for _, c := range m.Consts() {
		w.Fmt(", ")
		dapgx.WriteQuote(w, cor.Keyed(c.Name))
	}
	w.Dedent()
	return w.Fmt(");")
}

func WriteTable(w *dapgx.Writer, m *dom.Model) error {
	tname := fmt.Sprintf("%s.%s", checkIdent(m.Schema), warnIdent(m.Key()))
	w.Fmt("CREATE TABLE %s (", tname)
	w.Indent()
	params := m.Params()
	for i, p := range params {
		if i > 0 {
			w.WriteByte(',')
			if !w.Break() {
				w.WriteByte(' ')
			}
		}
		err := writeField(w, p, m.Elems[i])
		if err != nil {
			return err
		}
	}
	w.Dedent()
	w.Fmt(");")
	for i, e := range m.Elems {
		if e.Bits&dom.BitIdx == 0 {
			continue
		}
		pkey := params[i].Key
		name := fmt.Sprintf("%s_%s_idx", m.Key(), pkey)
		w.Fmt("\nCREATE INDEX %s on %s (%s);", name, tname, pkey)
	}
	if m.Object != nil {
		for _, ind := range m.Object.Indices {
			xtra, kind := "idx", "INDEX"
			if ind.Unique {
				xtra, kind = "uniq", "UNIQUE INDEX"
			}
			name := fmt.Sprintf("%s_%s_%s", m.Key(), strings.Join(ind.Keys, "_"), xtra)
			w.Fmt("\nCREATE %s %s on %s (%s);", kind, name, tname, strings.Join(ind.Keys, ", "))
		}
	}
	return nil
}

func warnIdent(name string) string {
	name, ok := dapgx.Unreserved(name)
	if !ok {
		// we explicitly want to log every reserved identifier whenever
		// we generate an sql schema so can change those names early.
		log.Printf("use of reserved postgresql ident %q", name)
		name = fmt.Sprintf("\"%s\"", name)
	}
	return name
}
func checkIdent(name string) string {
	name, ok := dapgx.Unreserved(name)
	if !ok {
		name = fmt.Sprintf("\"%s\"", name)
	}
	return name
}

func writeField(w *dapgx.Writer, p typ.Param, el *dom.Elem) error {
	key, err := dapgx.ColKey(p.Key, p.Type)
	if err != nil {
		return err
	}
	if key == "" {
		return writeEmbed(w, p.Type)
	}
	w.Fmt(checkIdent(key))
	w.Byte(' ')
	ts, err := dapgx.TypString(p.Type)
	if err != nil {
		return err
	}
	if ts == "int8" && el.Bits&dom.BitPK != 0 && el.Bits&dom.BitAuto != 0 {
		w.Fmt("serial8")
	} else {
		w.Fmt(ts)
	}
	if el.Bits&dom.BitPK != 0 {
		w.Fmt(" primary key")
		// TODO auto
	}
	null := p.Type.Kind&knd.None != 0 || p.Name != "" && p.Name[len(p.Name)-1] == '?'
	if null {
		w.Fmt(" null")
	} else if el.Bits&dom.BitPK == 0 {
		w.Fmt(" not null")
	}
	if el.Bits&dom.BitUniq != 0 {
		w.Fmt(" unique")
	}
	extra, _ := el.Extra.Key("def")
	if extra != nil && !extra.Nil() {
		w.Fmt(" default %s", extra)
	} else if !null && el.Bits&dom.BitOpt != 0 {
		switch ts {
		case "bool":
			w.Fmt(" default FALSE")
		case "text":
			w.Fmt(" default ''")
		case "int8":
			w.Fmt(" default 0")
		}
	}
	if el.Ref != "" {
		m := w.Project.Model(strings.ToLower(el.Ref))
		if m == nil {
			return fmt.Errorf("no model for %s", el.Ref)
		}
		name := fmt.Sprintf("%s.%s", m.Schema, checkIdent(m.Key()))
		w.Fmt(" references %s deferrable", name)
	}
	return nil
}

func writeEmbed(w *dapgx.Writer, t typ.Type) error {
	ref := t.Ref
	ps := strings.Split(ref, ".")
	if len(ps) > 1 {
		ref = ps[0] + "." + cor.Keyed(ps[1])
	}
	m := w.Project.Model(ref)
	if m == nil {
		return fmt.Errorf("no model for %s", t.Ref)
	}
	for i, p := range m.Params() {
		if i > 0 {
			w.Byte(',')
			if !w.Break() {
				w.Byte(' ')
			}
		}
		if p.Key == "" {
			writeEmbed(w, p.Type)
			continue
		}
		w.Fmt(checkIdent(p.Key))
		w.Byte(' ')
		ts, err := dapgx.TypString(p.Type)
		if err != nil {
			return err
		}
		w.Fmt(ts)
		if p.IsOpt() || p.Type.Kind&knd.None != 0 {
			w.Fmt(" null")
		} else {
			w.Fmt(" not null")
			// TODO implicit default
		}
	}
	return nil
}
