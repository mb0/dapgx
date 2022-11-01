package dapgx

import (
	"strings"
	"testing"

	"xelf.org/xelf/exp"
	"xelf.org/xelf/lib"
	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

func TestRender(t *testing.T) {
	reg := &lit.Reg{Cache: &lit.Cache{}}
	tests := []struct {
		el   string
		want string
	}{
		{`null`, `NULL`},
		{`true`, `TRUE`},
		{`false`, `FALSE`},
		{`23`, `23`},
		{`-42`, `-42`},
		{`'test'`, `'test'`},
		{`(raw 'test')`, `'test'::bytea`},
		{`(uuid '4d85fc61-398b-4886-a396-b67b6453e431')`,
			`'4d85fc61-398b-4886-a396-b67b6453e431'::uuid`},
		{`(time '2019-02-11')`, `'2019-02-11'::timestamptz`},
		{`(span '1h5m')`, `'1h5m'::interval`},
		{`[null true]`, `'[null,true]'::jsonb`},
		{`(list|int 1 2 3)`, `'{1,2,3}'::int8[]`},
		{`(list|str 'a' 'b' "'")`, `'{"a","b","''"}'::text[]`},
		{`{a:null b:true}`, `'{"a":null,"b":true}'::jsonb`},
		{`(or a b)`, `a OR b`},
		{`(not a b)`, `NOT a AND NOT b`},
		{`(and x v)`, `x != 0 AND v != ''`},
		{`(eq x y 1)`, `x = y AND x = 1`},
		{`(in x [1 2 3] [4 5])`, `x IN (1, 2, 3) OR x IN (4, 5)`},
		{`(in x t [4 5])`, `x = ANY(t) OR x IN (4, 5)`},
		{`(ni x t [4 5])`, `x != ALL(t) AND x NOT IN (4, 5)`},
		{`('hell' 'o W' 'orld')`, `CONCAT('hell', 'o W', 'orld')`},
		{`(sep ' | ' 'hell' 'o W' 'orld')`, `CONCAT('hell', ' | ', 'o W', ' | ', 'orld')`},
		{`(equal x 1)`, `(x = 1 AND pg_typeof(x) = pg_typeof(1))`},
		{`(gt x y 1)`, `x > y AND y > 1`},
		{`(if (eq v 'a') x (eq w 'b') y 1)`, `CASE WHEN v = 'a' THEN x WHEN w = 'b' THEN y ELSE 1 END`},
		{`(swt v 'a' x 'b' y 1)`, `CASE WHEN v = 'a' THEN x WHEN v = 'b' THEN y ELSE 1 END`},
		{`(df x null 3)`, `COALESCE(x, NULL, 3)`},
		{`(min x y 3)`, `LEAST(x, y, 3)`},
		{`(max x y 3)`, `GREATEST(x, y, 3)`},
		{`(add (add x 2) 3)`, `x + 2 + 3`},
		{`(add (mul x 2) 3)`, `x * 2 + 3`},
		{`(add 3 (mul x 2))`, `3 + x * 2`},
		{`(mul (add x 2) 3)`, `(x + 2) * 3`},
		{`(mul 3 (add x 2))`, `3 * (x + 2)`},
		{`(and (or a b) c)`, `(a OR b) AND c`},
		{`(or (and a b) c)`, `a AND b OR c`},
		{`(len 'test')`, `4`},
		{`(len [1 2 3])`, `3`},
		{`(len v)`, `octet_length(v)`},
		{`(len s)`, `jsonb_array_length(s)`},
		{`(len t)`, `array_length(t, 1)`},
		{`(len d)`, `(SELECT COUNT(*) FROM jsonb_object_keys(d))`},
	}
	env := &unresEnv{Par: lib.Std}
	env.add(typ.Bool, "a", "b", "c")
	env.add(typ.Str, "v", "w")
	env.add(typ.Int, "x", "y")
	env.add(typ.Dict, "d")
	env.add(typ.List, "s")
	env.add(typ.ListOf(typ.Int), "t")
	for _, test := range tests {
		ast, err := exp.Parse(reg, test.el)
		if err != nil {
			t.Errorf("parse %s err: %v", test.el, err)
			continue
		}
		p := exp.NewProg(nil, reg, env)
		el, err := p.Resl(p, ast, typ.Void)
		if err != nil {
			t.Errorf("resolve %s err: %v", test.el, err)
			continue
		}
		var b strings.Builder
		w := NewWriter(&b, nil, p, ExpEnv{})
		err = WriteExp(w, p, el)
		if err != nil {
			t.Errorf("render %s err: %+v", test.el, err)
			continue
		}
		got := b.String()
		if got != test.want {
			t.Errorf("%s want %s got %s", el, test.want, got)
		}
	}
}

type unresEnv struct {
	Par exp.Env
	Map map[string]typ.Type
}

func (e *unresEnv) add(t typ.Type, names ...string) {
	if e.Map == nil {
		e.Map = make(map[string]typ.Type)
	}
	for _, n := range names {
		e.Map[n] = t
	}
}
func (e *unresEnv) Parent() exp.Env { return e.Par }
func (e *unresEnv) Lookup(s *exp.Sym, k string, eval bool) (exp.Exp, error) {
	if t, ok := e.Map[k]; ok {
		s.Type = t
		return s, nil
	}
	return e.Par.Lookup(s, k, eval)
}
