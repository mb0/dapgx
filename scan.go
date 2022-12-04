package dapgx

import (
	"fmt"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"

	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

func ScanOne(reg lit.Regs, scal bool, mut lit.Mut, rows pgx.Rows) error {
	if rows.Next() {
		s, err := NewScanner(scal, rows)
		if err != nil {
			return err
		}
		err = s.Scan(mut)
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

func ScanMany(reg lit.Regs, scal bool, mut lit.Mut, rows pgx.Rows) (err error) {
	a, ok := mut.(lit.Appender)
	if !ok {
		return fmt.Errorf("expect appender result got %T", mut)
	}
	et := typ.ContEl(mut.Type())
	var s *Scanner
	for rows.Next() {
		el := reg.Zero(typ.Deopt(et))
		if s == nil {
			s, err = NewScanner(scal, rows)
			if err != nil {
				return err
			}
		}
		err = s.Scan(el)
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

// Scanner is a simplified xelf-aware Scanner for pgx rows. it avoids some hacks on my end,
// alleviate many extra type checks and has better null handling for my use-case.
type Scanner struct {
	rows pgx.Rows
	scal bool
	cols []scancol
}

type scancol struct {
	key    string
	decode Decoder
}

func NewScanner(scal bool, rows pgx.Rows) (*Scanner, error) {
	fds := rows.FieldDescriptions()
	if scal && len(fds) != 1 {
		return nil, fmt.Errorf("unexpected number of scalar fields, got %d", len(fds))
	}
	cols := make([]scancol, len(fds))
	for i, fd := range fds {
		dec := FieldDecoder(fd.DataTypeOID, fd.Format == pgtype.BinaryFormatCode)
		cols[i] = scancol{string(fd.Name), dec}
	}
	return &Scanner{rows: rows, scal: scal, cols: cols}, nil
}

func (s *Scanner) Scan(m lit.Mut) (err error) {
	vals := s.rows.RawValues()
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
			val, err = col.decode(raw)
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
