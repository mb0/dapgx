package qrypgx

import (
	"fmt"
	"strings"

	"xelf.org/daql/gen/genpg"
	"xelf.org/daql/qry"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/lit"
)

func genQuery(p *exp.Prog, q *Query) (string, []genpg.Param, error) {
	b := &strings.Builder{}
	w := genpg.NewWriter(b, p, &jobTranslator{q.Alias})
	err := genSelect(w, p, q.Alias, q)
	if err != nil {
		return "", nil, err
	}
	return b.String(), w.Params, nil
}

func genSelect(w *genpg.Writer, p *exp.Prog, alias Alias, q *Query) error {
	w.WriteString("SELECT ")
	var suf string
	if q.Kind&KindCount != 0 {
		w.WriteString("count(*)")
		if q.Job.Lim != 0 || q.Job.Off != 0 {
			suf = ") _"
			w.WriteString(" FROM (SELECT TRUE")
		}
	} else if q.Kind&KindScalar != 0 {
		for _, c := range q.Cols {
			err := w.WriteExp(c.Job, c.Exp)
			if err != nil {
				return err
			}
		}
	} else if len(q.Cols) == 0 {
		w.WriteString("FALSE")
	} else {
		for i, c := range q.Cols {
			if i > 0 {
				w.WriteString(", ")
			}
			if c.Sub != nil {
				if c.Sub.Kind&KindInlined != 0 {
					w.WriteString("(")
					if c.Sub.Kind&(KindScalar|KindCount) == KindScalar {
						sca := c.Sub.Cols[0]
						if c.Sub.Kind&KindMany != 0 {
							w.WriteString("SELECT jsonb_agg(")
							err := w.WriteExp(c.Job, sca.Exp)
							if err != nil {
								return err
							}
							w.WriteString(")")
						}
						err := genFrom(w, alias, c.Sub, 0)
						if err != nil {
							return err
						}
						_, err = genWhere(w, c.Sub, 0)
						if err != nil {
							return err
						}
						err = genCommon(w, c.Sub.Job)
						if err != nil {
							return err
						}
					} else {
						if c.Sub.Kind&KindCount == 0 {
							w.WriteString("SELECT jsonb_agg(_) FROM (")
						}
						err := genSelect(w, p, alias, c.Sub)
						if err != nil {
							return err
						}
						if c.Sub.Kind&(KindScalar|KindCount) == 0 {
							w.WriteString(") _")
						}
					}
					w.WriteString(") as ")
				} else {
					return fmt.Errorf("not implemented")
				}
			} else if c.Exp != nil {
				err := w.WriteExp(c.Job, c.Exp)
				if err != nil {
					return err
				}
				w.WriteString(" as ")
			} else if as := q.Alias[c.Job]; as != "" {
				w.WriteString(as)
				w.WriteByte('.')
			}
			w.WriteString(c.Key)
		}
	}
	// TODO wrap json
	err := genFrom(w, alias, q, 0)
	if err != nil {
		return err
	}
	_, err = genWhere(w, q, 0)
	if err != nil {
		return err
	}
	err = genCommon(w, q.Job)
	if err != nil {
		return err
	}
	if suf != "" {
		w.WriteString(suf)
	}
	return nil
}

type jobTranslator struct {
	Alias
}

func (jt *jobTranslator) Translate(p *exp.Prog, env exp.Env, s *exp.Sym) (string, lit.Val, error) {
	j := qry.FindJob(s.Env)
	if j == nil {
		panic(fmt.Errorf("translate no job env found %T", s.Env))
		return genpg.ExpEnv{}.Translate(p, env, s)
	}
	n := s.Rel[1:]
	sp := strings.SplitN(n, ".", 2)
	if len(sp) == 1 {
		// TODO only check if inline or joined query
		f, _ := j.Field(sp[0])
		for f != nil {
			return jt.ColRef(j, n), nil, nil
		}
	}
	return "", nil, fmt.Errorf("no selection for %q", s.Sym)
}

func genCommon(w *genpg.Writer, j *qry.Job) error {
	if len(j.Ord) > 0 {
		w.WriteString(" ORDER BY ")
		for i, ord := range j.Ord {
			if i > 0 {
				w.WriteString(", ")
			}
			key := ord.Key
			if key[0] == '.' {
				key = key[1:]
			}
			w.WriteString(key)
			if ord.Desc {
				w.WriteString(" DESC")
			}
		}
	}
	lim := j.Lim
	if j.Kind == qry.KindOne {
		lim = 1
	}
	if lim > 0 {
		w.Fmt(" LIMIT %d", lim)
	}
	if j.Off > 0 {
		w.Fmt(" OFFSET %d", j.Off)
	}
	return nil
}
func genFrom(w *genpg.Writer, a Alias, q *Query, i int) error {
	if i > 0 {
		w.WriteString(", ")
	} else {
		w.WriteString(" FROM ")
	}
	w.WriteString(a.AsRef(q.Job))
	i++
	for _, qj := range q.Join {
		err := genFrom(w, a, qj, i)
		if err != nil {
			return err
		}
	}
	return nil
}
func genWhere(w *genpg.Writer, q *Query, i int) (_ int, err error) {
	for _, whr := range q.Whr {
		if i == 0 {
			w.WriteString(" WHERE ")
		} else {
			w.WriteString(" AND ")
		}
		i++
		err = w.WriteExp(q.Job, whr)
		if err != nil {
			return i, err
		}
	}
	for _, qj := range q.Join {
		i, err = genWhere(w, qj, i)
		if err != nil {
			return i, err
		}
	}
	return i, nil
}
