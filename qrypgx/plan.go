package qrypgx

import (
	"fmt"
	"strings"

	"xelf.org/daql/qry"
	"xelf.org/xelf/cor"
)

type Kind uint

const (
	KindMany = 1 << iota
	KindOne
	KindCount
	KindScalar
	KindJoin
	KindJoined
	KindInline
	KindInlined
	KindJSON
	KindAlias
)

type Alias map[*qry.Job]string

func (a Alias) AsRef(j *qry.Job) string {
	if as := a[j]; as != "" {
		return fmt.Sprintf("%s %s", j.Ref, as)
	}
	return j.Ref
}
func (a Alias) ColRef(j *qry.Job, k string) string {
	if as := a[j]; as != "" {
		return fmt.Sprintf("%s.%s", as, k)
	}
	return k
}

type Column struct {
	*qry.Job
	*qry.Field
	Sub *Query
	Key string
}

type Query struct {
	Kind Kind
	Alias
	*qry.Job
	Parent *Query
	Join   []*Query
	Deps   []*qry.Job
	Cols   []*Column
}

func (q *Query) DependsOn(j *qry.Job) bool {
	for _, d := range q.Deps {
		if d == j {
			return true
		}
	}
	return false
}

type Batch struct {
	*qry.Doc
	List []*Query
	All  []*Query
}

// Analyse returns the next batch of queries for doc.
func Analyse(bend qry.Backend, d *qry.Doc) (*Batch, error) {
	last := 0
	b := &Batch{Doc: d}
	for _, j := range d.Root {
		if j.Bend != bend || j.Model == nil || j.Val != nil { // other backend or already evaluated
			continue
		}
		a := newAliaser()
		q, err := b.newQuery(j, nil, a)
		if err != nil {
			return nil, err
		}
		// root tasks are always distinct jobs, for now
		b.List = append(b.List, q)
		err = b.analyseQuery(q, a)
		if err != nil {
			return nil, err
		}
		for _, q := range b.All[last:] {
			if q.Kind&KindAlias != 0 {
				as := a.addAlias(q.Job)
				if as == "FAIL" {
					return nil, fmt.Errorf("no alias for %s", q.Ref)
				}
			}
			last++
		}
	}
	return b, nil
}
func (b *Batch) newQuery(j *qry.Job, par *Query, a aliaser) (*Query, error) {
	q := &Query{Job: j, Alias: a.Alias, Parent: par}
	b.All = append(b.All, q)
	if j.Model == nil {
		return q, nil
	}
	s := strings.SplitN(j.Ref, ".", 2)
	if len(s) < 2 {
		return nil, fmt.Errorf("unqualified query %s", j.Ref)
	}
	switch j.Kind {
	case qry.KindCount:
		q.Kind |= KindCount | KindScalar
	case qry.KindOne:
		q.Kind |= KindOne
	case qry.KindMany:
		q.Kind |= KindMany
	}
	for _, s := range j.Sel.Fields {
		if s.Name == "_" {
			q.Kind |= KindScalar
		}
	}
	return q, nil
}

func (b *Batch) analyseQuery(q *Query, a aliaser) error {
	// TODO look for deps in whr exprs
	for _, f := range q.Job.Sel.Fields {
		a.block(f.Key)
		c := &Column{q.Job, f, nil, f.Key}
		q.Cols = append(q.Cols, c)
		if f.Exp == nil {
			continue
		}
		if f.Sub == nil {
			continue
		}
		sub, err := b.newQuery(f.Sub, q, a)
		if err != nil {
			return err
		}
		q.Kind |= KindAlias
		sub.Kind |= KindAlias
		err = b.analyseQuery(sub, a)
		if err != nil {
			return err
		}
		if sub.Kind&KindOne != 0 {
			sub.Kind |= KindJoined
			if sub.Kind&KindScalar != 0 {
				sub.Cols[0].Key = f.Key

			}
			q.Kind |= KindJoin
			q.Cols = append(q.Cols[:len(q.Cols)-1], sub.Cols...)
			q.Join = append(q.Join, sub)
			continue
		}
		q.Kind |= KindInline
		sub.Kind |= KindInlined
		if sub.Kind&KindMany == 0 {
			sub.Kind |= KindJSON
		}
		c.Sub = sub
	}
	return nil
}

type aliaser struct {
	keys map[string]struct{}
	Alias
}

func newAliaser() aliaser {
	return aliaser{make(map[string]struct{}), make(Alias)}
}

func (a aliaser) block(k string) { a.keys[k] = struct{}{} }

func (a aliaser) addAlias(j *qry.Job) string {
	const digits = "1234567890"
	n := cor.Keyed(j.Model.Name)
	for _, k := range [...]string{n[:1], n} {
		if a.try(k, j) {
			return k
		}
		for i := 0; i < 10; i++ {
			if k1 := k + digits[i:i+1]; a.try(k1, j) {
				return k1
			}
		}
	}
	return "FAIL"
}

func (a aliaser) try(k string, j *qry.Job) bool {
	if _, ok := a.keys[k]; !ok {
		a.keys[k] = struct{}{}
		a.Alias[j] = k
		return true
	}
	return false
}
