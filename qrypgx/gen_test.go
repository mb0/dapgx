package qrypgx

import (
	"testing"

	"xelf.org/daql/dom/domtest"
	"xelf.org/daql/qry"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/lib/extlib"
	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

func TestGenQuery(t *testing.T) {
	reg := &lit.Reg{}
	f := domtest.Must(domtest.ProdFixture(reg))
	b := New(nil, &f.Project)
	tests := []struct {
		raw  string
		want []string
	}{
		{`(#prod.cat)`, []string{`SELECT count(*) FROM prod.cat`}},
		{`(*prod.cat)`, []string{`SELECT id, name FROM prod.cat`}},
		{`(?prod.cat)`, []string{`SELECT id, name FROM prod.cat LIMIT 1`}},
		{`(?prod.cat _:name)`, []string{`SELECT name FROM prod.cat LIMIT 1`}},
		{`(?prod.cat off:2)`, []string{`SELECT id, name FROM prod.cat LIMIT 1 OFFSET 2`}},
		{`(*prod.cat _ id;)`, []string{`SELECT id FROM prod.cat`}},
		{`(*prod.cat (gt .name 'B'))`, []string{
			`SELECT id, name FROM prod.cat WHERE name > 'B'`,
		}},
		{`(*prod.cat asc:name)`, []string{
			`SELECT id, name FROM prod.cat ORDER BY name`,
		}},
		{`(*prod.cat _ id; label:('label: ' .name))`, []string{
			`SELECT id, CONCAT('label: ', name) as label FROM prod.cat`,
		}},
		{`(?prod.prod (eq .name 'A')
			_ name; cname:(?prod.cat (eq .id ..cat) _:name)
		)`, []string{`SELECT p.name, c.name as cname FROM prod.prod p, prod.cat c ` +
			`WHERE p.name = 'A' AND c.id = p.cat LIMIT 1`,
		}},
		{`(*prod.cat (or (eq .name 'b') (eq .name 'c'))
			+ prods:(#prod.prod (eq .cat ..id))
		)`, []string{`SELECT c.id, c.name, ` +
			`(SELECT count(*) FROM prod.prod p WHERE p.cat = c.id) as prods ` +
			`FROM prod.cat c WHERE c.name = 'b' OR c.name = 'c'`,
		}},
		// TODO could use a join for one nested query
		// SELECT c.id, c.name, jsonb_agg(p.id) FILTER (WHERE p is not null)
		// FROM prod.cat c left join prod.prod p ON c.id = p.cat
		// WHERE c.name = 'b' OR c.name = 'c' GROUP BY c.id
		{`(*prod.cat (or (eq .name 'b') (eq .name 'c'))
			+ prods:(*prod.prod (eq .cat ..id) _:id)
		)`, []string{`SELECT c.id, c.name, ` +
			`(SELECT jsonb_agg(p.id) FROM prod.prod p WHERE p.cat = c.id) as prods ` +
			`FROM prod.cat c WHERE c.name = 'b' OR c.name = 'c'`,
		}},
		// TODO could use a join again or for more complex situations resort to multiple
		// queries that are stitched back together
		// SELECT id, name FROM prod.cat WHERE name = 'b' OR name = 'c'
		// SELECT p.cat, jsonb_agg(p) FROM prod.prod p WHERE p.cat in
		// (SELECT id FROM prod.cat WHERE name = 'b' OR name = 'c') group by p.cat
		{`(*prod.cat (or (eq .name 'b') (eq .name 'c'))
			+ prods:(*prod.prod (eq .cat ..id) _ id; name;)
		)`, []string{`SELECT c.id, c.name, (SELECT jsonb_agg(_) FROM ` +
			`(SELECT p.id, p.name FROM prod.prod p WHERE p.cat = c.id) _) as prods ` +
			`FROM prod.cat c WHERE c.name = 'b' OR c.name = 'c'`,
		}},
		{`(?prod.prod (eq .id 1)
			_ name; co:(?prod.cat (eq .id ..cat))
		)`, []string{`SELECT p.name, c.id, c.name FROM prod.prod p, prod.cat c ` +
			`WHERE p.id = 1 AND c.id = p.cat LIMIT 1`,
		}},
		{`(?prod.prod (eq .id 1)
			_ name; cn:(?prod.cat (eq .id ..cat) _:name)
		)`, []string{`SELECT p.name, c.name as cn FROM prod.prod p, prod.cat c ` +
			`WHERE p.id = 1 AND c.id = p.cat LIMIT 1`,
		}},
		{`(?prod.prod (eq .id 1)
			_ name; c:(?prod.cat (eq .id ..cat) _:name)
		)`, []string{`SELECT p.name, c1.name as c FROM prod.prod p, prod.cat c1 ` +
			`WHERE p.id = 1 AND c1.id = p.cat LIMIT 1`,
		}},
	}
	q := qry.New(reg, extlib.Std, b)
	for _, test := range tests {
		ast, err := exp.Parse(reg, test.raw)
		if err != nil {
			t.Errorf("parse %s error: %w", test.raw, err)
			continue
		}
		d := &qry.Doc{Qry: q}
		c := exp.NewProg(reg, d, ast)
		c.Exp, err = c.Resl(c.Root, c.Exp, typ.Void)
		if err != nil {
			t.Errorf("resolve %s error %+v", test.raw, err)
			continue
		}
		batch, err := Analyse(b, d)
		if err != nil {
			t.Errorf("analyse project: %v", err)
			continue
		}
		var res []string
		for _, q := range batch.List {
			qs, _, err := genQuery(b.Project, c, q)
			if err != nil {
				t.Errorf("gen queries %s: %v", test.raw, err)
				continue
			}
			res = append(res, qs)
		}
		if len(res) != len(test.want) {
			t.Errorf("want %d queries got %d", len(test.want), len(res))
			continue
		}
		for i, got := range res {
			if got != test.want[i] {
				t.Errorf("for %s\n\twant %s\n\t got %s", test.raw, test.want[i], got)
			}
		}
	}
}
