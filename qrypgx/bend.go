package qrypgx

import (
	"fmt"
	"log"
	"time"

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
	for _, q := range batch.List {
		// generate each query and add them to a batch
		err := b.execQuery(p, q)
		if err != nil {
			return nil, err
		}
	}
	// query the batch and read in the results
	// update the result value of the jobs
	return j.Val, nil
}
func (b *Backend) execQuery(p *exp.Prog, q *Query) error {
	qs, ps, err := genQuery(b.Project, p, q)
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() {
		log.Printf("query %s took %s", qs, time.Now().Sub(start))
	}()
	var args []lit.Val
	if len(ps) != 0 {
		args = make([]lit.Val, 0, len(ps))
		for _, p := range ps {
			if p.Value != nil {
				args = append(args, p.Value)
				continue
			}
			return fmt.Errorf("unexpected external param %+v", p)
		}
	}
	return b.DB.AcquireFunc(dapgx.BG, func(c *pgxpool.Conn) error {
		rows, err := dapgx.Query(dapgx.BG, c.Conn(), qs, args)
		if err != nil {
			return fmt.Errorf("query %s: %w", qs, err)
		}
		defer rows.Close()
		mut, err := p.Reg.Zero(typ.Deopt(q.Res))
		if err != nil {
			return fmt.Errorf("query %s: %w", qs, err)
		}
		q.Val = mut
		scan := dapgx.ScanMany
		if q.Kind&KindMany == 0 {
			scan = dapgx.ScanOne
		}
		err = scan(p.Reg, q.Kind&KindScalar != 0, mut, rows)
		if err != nil {
			return fmt.Errorf("query %s: %w", qs, err)
		}
		return nil
	})
}
