package dompgx

import (
	"context"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/daql/dom"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
)

func CreateProject(ctx context.Context, db *pgxpool.Pool, p *dom.Project) error {
	return dapgx.WithTx(ctx, db, func(tx dapgx.PC) error {
		err := dropProject(ctx, tx, p)
		if err != nil {
			return err
		}
		for _, s := range p.Schemas {
			_, err = tx.Exec(ctx, "CREATE SCHEMA "+s.Name)
			if err != nil {
				return err
			}
			for _, m := range s.Models {
				err = CreateModel(ctx, tx, p, s, m)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func DropProject(ctx context.Context, db *pgxpool.Pool, p *dom.Project) error {
	return dapgx.WithTx(ctx, db, func(tx dapgx.PC) error {
		return dropProject(ctx, tx, p)
	})
}

func CreateModel(ctx context.Context, tx dapgx.C, p *dom.Project, s *dom.Schema, m *dom.Model) error {
	switch m.Kind.Kind {
	case knd.Bits:
		return nil
	case knd.Enum:
		return createModel(ctx, tx, p, m, WriteEnum)
	case knd.Obj:
		if hasFlag(m.Extra, "backup") || hasFlag(m.Extra, "topic") {
			err := createModel(ctx, tx, p, m, WriteTable)
			if err != nil {
				return err
			}
		}
		// TODO indices
		return nil
	case knd.Func:
		return nil
	}
	return fmt.Errorf("unexpected model kind %s", m.Kind)
}

func hasFlag(d *lit.Dict, key string) bool {
	v, err := d.Key(key)
	return err == nil && !v.Zero()
}

func createModel(ctx context.Context, tx dapgx.C, p *dom.Project, m *dom.Model, f func(*dapgx.Writer, *dom.Model) error) error {
	var b strings.Builder
	w := dapgx.NewWriter(&b, p, nil, dapgx.ExpEnv{})
	err := f(w, m)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, b.String())
	return err
}

func dropProject(ctx context.Context, tx dapgx.C, p *dom.Project) error {
	for i := len(p.Schemas) - 1; i >= 0; i-- {
		s := p.Schemas[i]
		_, err := tx.Exec(ctx, "DROP SCHEMA IF EXISTS "+s.Name+" CASCADE")
		if err != nil {
			return err
		}
	}
	return nil
}

func CopyFrom(ctx context.Context, db *pgxpool.Pool, reg *lit.Reg, s *dom.Schema, fix lit.Keyed) error {
	return dapgx.WithTx(ctx, db, func(tx dapgx.PC) error {
		for _, kv := range fix {
			m := s.Model(kv.Key)
			cols := make([]string, 0, len(m.Elems))
			for _, f := range m.Elems {
				cols = append(cols, cor.Keyed(f.Name))
			}
			_, err := tx.CopyFrom(ctx, pgx.Identifier{m.Qual(), m.Key()}, cols, &litCopySrc{
				Vals: *kv.Val.(*lit.Vals), reg: reg, m: m,
			})
			if err != nil {
				return fmt.Errorf("copy from: %w", err)
			}
		}
		return nil
	})
}

type litCopySrc struct {
	lit.Vals
	reg *lit.Reg
	m   *dom.Model
	nxt int
	err error
	res interface{}
}

func (c *litCopySrc) Next() bool {
	c.nxt++
	return c.err == nil && c.nxt <= len(c.Vals)
}
func (c *litCopySrc) Values() ([]interface{}, error) {
	el, err := c.Idx(c.nxt - 1)
	if err != nil {
		c.err = err
		return nil, err
	}
	prx, err := c.reg.Zero(c.m.Type())
	if err != nil {
		c.err = err
		return nil, err
	}
	if l, ok := el.(*lit.Vals); ok {
		vs := make([]lit.Val, 0, len(*l))
		for i, v := range *l {
			p := c.m.Elems[i]
			vp, err := c.reg.Zero(p.Type)
			if err != nil {
				c.err = err
				return nil, c.err
			}
			err = vp.Assign(v)
			if err != nil {
				c.err = err
				return nil, c.err
			}
			vs = append(vs, vp)
		}
		el = &lit.Obj{Typ: c.m.Type(), Vals: vs}
	}
	err = prx.Assign(el)
	if err != nil {
		c.err = fmt.Errorf("assign %s to %s: %w", el, prx.Type(), err)
		return nil, c.err
	}
	k, ok := prx.(lit.Keyr)
	if !ok {
		c.err = fmt.Errorf("expect keyer got %T", el)
		return nil, c.err
	}
	res := make([]interface{}, 0, len(c.m.Elems))
	for _, f := range c.m.Elems {
		el, err = k.Key(f.Key())
		if err != nil {
			c.err = fmt.Errorf("get key %v from %s: %w", f.Key(), prx.Type(), err)
			return nil, c.err
		}
		if el == nil {
			c.err = fmt.Errorf("expect proxy got %T", el)
			return nil, c.err
		}
		res = append(res, el.Value())
	}
	return res, nil
}

func (c *litCopySrc) Err() error {
	return c.err
}
