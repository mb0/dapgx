package qrypgx

import (
	"fmt"
	"log"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/daql/dom"
	"xelf.org/daql/mig"
	"xelf.org/daql/qry"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

type Backend struct {
	DB *pgxpool.Pool
	*dom.Project
	*mig.Version
	tables map[string]*dom.Model
}

func New(db *pgxpool.Pool, proj *dom.Project) *Backend {
	tables := make(map[string]*dom.Model, len(proj.Schemas)*16)
	for _, s := range proj.Schemas {
		for _, m := range s.Models {
			if m.Kind.Kind != knd.Obj {
				continue
			}
			tables[m.Qualified()] = m
		}
	}
	return &Backend{DB: db, Project: proj, tables: tables}
}

func (b *Backend) Proj() *dom.Project { return b.Project }
func (b *Backend) Exec(p *exp.Prog, j *qry.Job) (lit.Val, error) {
	if j.Val != nil {
		return j.Val, nil
	}
	// retrieve all ready queries for this backend
	batch, err := Analyse(b, j.Doc)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	for _, q := range batch.List {
		// generate each query and add them to a batch
		err := b.execQuery(p, q)
		if err != nil {
			return nil, err
		}
	}
	log.Printf("exec batch %d took %s", len(batch.List), time.Now().Sub(start))
	// query the batch and read in the results
	// update the result value of the jobs
	return j.Val, nil
}
func (b *Backend) execQuery(p *exp.Prog, q *Query) error {
	qs, ps, err := genQuery(p, q)
	if err != nil {
		return err
	}
	var args []interface{}
	if len(ps) != 0 {
		args = make([]interface{}, 0, len(ps))
		for _, p := range ps {
			if p.Value != nil {
				args = append(args, p.Value)
				continue
			}
			return fmt.Errorf("unexpected external param %+v", p)
		}
	}
	start := time.Now()
	log.Printf("query %s", qs)
	rows, err := b.DB.Query(dapgx.BG, qs, args...)
	if err != nil {
		return fmt.Errorf("query %s: %w", qs, err)
	}
	defer rows.Close()
	mut, err := p.Reg.Zero(typ.Deopt(q.Res))
	if err != nil {
		return fmt.Errorf("query %s: %w", qs, err)
	}
	start = time.Now()
	defer func() {
		log.Printf("scan took %s", time.Now().Sub(start))
	}()
	q.Val = mut
	switch {
	case q.Kind&KindScalar != 0:
		return b.scanScalar(q, mut, rows)
	case q.Kind&KindOne != 0:
		return b.scanOne(p, q, mut, rows)
	case q.Kind&KindMany != 0:
		return b.scanMany(p, q, mut, rows)
	}
	return fmt.Errorf("unexpected query kind for %s", q.Ref)
}

func (b *Backend) scanScalar(q *Query, mut lit.Mut, rows pgx.Rows) error {
	if !rows.Next() {
		return fmt.Errorf("no result for query %s", q.Ref)
	}
	err := rows.Scan(mut.Ptr())
	if err != nil {
		return fmt.Errorf("scan row for query %s: %w", q.Ref, err)
	}
	if rows.Next() {
		return fmt.Errorf("additional results for query %s", q.Ref)
	}
	return rows.Err()
}
func (b *Backend) scanOne(p *exp.Prog, q *Query, mut lit.Mut, rows pgx.Rows) error {
	if rows.Next() {
		err := b.scanRow(p, q, mut, rows)
		if err != nil {
			return err
		}
	}
	if rows.Next() {
		return fmt.Errorf("additional results for query %s", q.Ref)
	}
	return rows.Err()
}
func (b *Backend) scanMany(p *exp.Prog, q *Query, mut lit.Mut, rows pgx.Rows) error {
	a, ok := mut.(lit.Apdr)
	if !ok {
		return fmt.Errorf("expect appender result got %T", mut)
	}
	et := typ.ContEl(mut.Type())
	for rows.Next() {
		el, err := p.Reg.Zero(typ.Deopt(et))
		if err != nil {
			return err
		}
		err = b.scanRow(p, q, el, rows)
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
func (b *Backend) scanRow(p *exp.Prog, q *Query, mut lit.Mut, rows pgx.Rows) (err error) {
	k, ok := mut.(lit.Keyr)
	if !ok {
		panic(fmt.Errorf("scanRow expect keyr got %T %s", mut, mut))
	}
	opts := make([]*lit.OptMut, 0, len(q.Cols))
	args := make([]interface{}, 0, len(q.Cols))
	for _, c := range q.Cols {
		var v lit.Val
		if c.Job == q.Job || c.Job.Kind&KindInlined != 0 {
			v, err = k.Key(c.Key)
			if err != nil {
				return fmt.Errorf("scan %s %s: %w", q.Ref, c.Key, err)
			}
		} else { // joined table
			v, err = k.Key(c.Key)
			if err != nil {
				return fmt.Errorf("scan %s %s: %w", q.Ref, c.Key, err)
			}
			if c.Job.Kind&KindScalar == 0 {
				sub, ok := v.(lit.Keyr)
				if !ok {
					return fmt.Errorf("scan join expect keyr got %T", v)
				}
				v, err = sub.Key(c.Key)
				if err != nil {
					return fmt.Errorf("scan join field %s: %w", c.Key, err)
				}
			} else {
				log.Printf("scalar join mut %s %T", v, v)
			}
		}
		p, ok := v.(lit.Mut)
		if !ok {
			return fmt.Errorf("scan %s expect proxy got %T from %s", q.Ref, v, mut.Type())
		}
		o, _ := v.(*lit.OptMut)
		opts = append(opts, o)
		args = append(args, p.Ptr())
	}
	err = rows.Scan(args...)
	if err != nil {
		return fmt.Errorf("scan %s: %w", q.Ref, err)
	}

	for _, opt := range opts {
		if opt != nil {
			opt.Null = opt.Mut.Zero()
		}
	}
	return nil
}
