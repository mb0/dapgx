package dompgx

import (
	"fmt"
	"io"
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
	w.WriteString(w.Header)
	w.WriteString("BEGIN;\n\n")
	err := WriteSchema(w, s)
	if err != nil {
		return fmt.Errorf("render file %s error: %v", fname, err)
	}
	w.WriteString("COMMIT;\n")
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
	w.Fmt("CREATE SCHEMA %s;\n\n", s.Name)
	for _, m := range ms {
		switch m.Kind.Kind {
		case knd.Enum:
			err = WriteEnum(w, m)
		default:
			err = WriteTable(w, m)
		}
		if err != nil {
			return err
		}
		w.WriteString(";\n\n")
	}
	return nil
}

func WriteEnum(w *dapgx.Writer, m *dom.Model) error {
	w.WriteString("CREATE TYPE ")
	w.WriteString(m.Qualified())
	w.WriteString(" AS ENUM (")
	w.Indent()
	for i, c := range m.Consts() {
		if i > 0 {
			w.WriteByte(',')
			if !w.Break() {
				w.WriteByte(' ')
			}
		}
		dapgx.WriteQuote(w, cor.Keyed(c.Name))
	}
	w.Dedent()
	return w.WriteByte(')')
}

func WriteTable(w *dapgx.Writer, m *dom.Model) error {
	w.WriteString("CREATE TABLE ")
	w.WriteString(m.Qualified())
	w.WriteString(" (")
	w.Indent()
	for i, p := range m.Params() {
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
	return w.WriteByte(')')
}

func writeField(w *dapgx.Writer, p typ.Param, el *dom.Elem) error {
	key := p.Key
	if key == "" {
		switch p.Type.Kind & knd.Any {
		case knd.Bits, knd.Enum:
			split := strings.Split(typ.Name(p.Type), ".")
			key = split[len(split)-1]
		case knd.Obj:
			return writeEmbed(w, p.Type)
		default:
			return fmt.Errorf("unexpected embedded field type %s", p.Type)
		}
	}
	w.WriteString(key)
	w.WriteByte(' ')
	ts, err := dapgx.TypString(p.Type)
	if err != nil {
		return err
	}
	if ts == "int8" && el.Bits&dom.BitPK != 0 && el.Bits&dom.BitAuto != 0 {
		w.WriteString("serial8")
	} else {
		w.WriteString(ts)
	}
	if el.Bits&dom.BitPK != 0 {
		w.WriteString(" PRIMARY KEY")
		// TODO auto
	} else if el.Bits&dom.BitOpt != 0 || p.Type.Kind&knd.None != 0 {
		w.WriteString(" NULL")
	} else {
		w.WriteString(" NOT NULL")
	}
	// TODO default
	// TODO references
	return nil
}

func writeEmbed(w *dapgx.Writer, t typ.Type) error {
	m := w.Project.Model(typ.Name(t))
	if m == nil {
		return fmt.Errorf("no model for %s", typ.Name(t))
	}
	for i, p := range m.Params() {
		if i > 0 {
			w.WriteByte(',')
			if !w.Break() {
				w.WriteByte(' ')
			}
		}
		if p.Key == "" {
			writeEmbed(w, p.Type)
			continue
		}
		w.WriteString(p.Key)
		w.WriteByte(' ')
		ts, err := dapgx.TypString(p.Type)
		if err != nil {
			return err
		}
		w.WriteString(ts)
		if p.IsOpt() || p.Type.Kind&knd.None != 0 {
			w.WriteString(" NULL")
		} else {
			w.WriteString(" NOT NULL")
			// TODO implicit default
		}
	}
	return nil
}
