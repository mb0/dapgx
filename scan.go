package dapgx

import (
	"fmt"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"

	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

func ScanOne(reg *lit.Reg, scal bool, mut lit.Mut, rows pgx.Rows) error {
	if rows.Next() {
		s, err := newScanner(reg, scal, rows)
		if err != nil {
			return err
		}
		err = s.scan(mut)
		if err != nil {
			return err
		}
		if rows.Next() {
			return fmt.Errorf("additional query results")
		}
	} else if scal {
		return fmt.Errorf("no scalar query result")
	}
	return rows.Err()
}

func ScanMany(reg *lit.Reg, scal bool, mut lit.Mut, rows pgx.Rows) error {
	a, ok := mut.(lit.Apdr)
	if !ok {
		return fmt.Errorf("expect appender result got %T", mut)
	}
	et := typ.ContEl(mut.Type())
	var s *scanner
	for rows.Next() {
		el, err := reg.Zero(typ.Deopt(et))
		if err != nil {
			return err
		}
		if s == nil {
			s, err = newScanner(reg, scal, rows)
			if err != nil {
				return err
			}
		}
		err = s.scan(el)
		if err != nil {
			return err
		}
		err = a.Append(el)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

// scanner is a simplified xelf-aware scanner for pgx rows. it avoids some hacks on my end,
// alleviate many extra type checks and has better null handling for my use-case.
type scanner struct {
	pgx.Rows
	reg  *lit.Reg
	scal bool
	cols []scancol
}

type scancol struct {
	key    string
	decode Decoder
}

func newScanner(reg *lit.Reg, scal bool, rows pgx.Rows) (*scanner, error) {
	fds := rows.FieldDescriptions()
	if scal && len(fds) != 1 {
		return nil, fmt.Errorf("unexpected number of scalar fields, got %d", len(fds))
	}
	cols := make([]scancol, len(fds))
	for i, fd := range fds {
		dec := FieldDecoder(fd.DataTypeOID, fd.Format == pgtype.BinaryFormatCode)
		cols[i] = scancol{string(fd.Name), dec}
	}
	return &scanner{Rows: rows, reg: reg, scal: scal, cols: cols}, nil
}

func (s *scanner) scan(m lit.Mut) (err error) {
	vals := s.RawValues()
	if len(vals) != len(s.cols) {
		return fmt.Errorf("unexpected number of row values, got %d want %d",
			len(vals), len(s.cols))
	}
	var k lit.Keyr
	if !s.scal {
		var ok bool
		k, ok = m.(lit.Keyr)
		if !ok {
			return fmt.Errorf("scan expect keyr got %T", m)
		}
	}
	for i, raw := range vals {
		col := s.cols[i]
		var val lit.Val
		if raw != nil {
			val, err = col.decode(raw, s.reg)
			if err != nil {
				return err
			}
		}
		if s.scal {
			err = m.Assign(val)
		} else {
			err = k.SetKey(col.key, val)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
