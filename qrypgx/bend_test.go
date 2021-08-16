package qrypgx

import (
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/dapgx/dompgx"
	"xelf.org/daql/dom/domtest"
	"xelf.org/daql/qry"
	"xelf.org/xelf/bfr"
	"xelf.org/xelf/lib/extlib"
	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

var testDsn = "host=/var/run/postgresql dbname=daql"

func getBackend(reg *lit.Reg, db *pgxpool.Pool) (qry.Backend, error) {
	f := domtest.Must(domtest.ProdFixture(reg))
	if db != nil {
		err := dompgx.CreateProject(db, &f.Project)
		if err != nil {
			return nil, err
		}
		s := f.Schema("prod")
		err = dompgx.CopyFrom(db, reg, s, f.Fix)
		if err != nil {
			return nil, err
		}
	}
	b := New(db, &f.Project)
	return b, nil
}

func TestQry(t *testing.T) {
	db, err := dapgx.Open(testDsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reg := &lit.Reg{}
	b, err := getBackend(reg, db)
	if err != nil {
		t.Fatal(err)
	}
	db.QueryRow(dapgx.BG, "select true").Scan(new(bool))
	tests := []struct {
		Raw  string
		Want string
	}{
		{`(#prod.cat)`, `7`},
		{`(#prod.prod)`, `6`},
		{`([] (#prod.cat) (#prod.prod))`, `[7 6]`},
		{`({} cats:(#prod.cat) prods:(#prod.prod))`, `{cats:7 prods:6}`},
		{`(#prod.cat off:5 lim:5)`, `2`},
		{`(#prod.prod (eq .cat $int1))`, `2`},
		{`(?prod.cat)`, `{id:25 name:'y'}`},
		{`(?$list)`, `'a'`},
		{`(#$list)`, `3`},
		{`(#prod.cat (gt .name 'd'))`, `3`},
		{`(?prod.cat (eq .id 1) _:name)`, `'a'`},
		{`(?prod.cat (eq .id $int1) _:name)`, `'a'`},
		{`(?prod.cat (eq .name 'a'))`, `{id:1 name:'a'}`},
		{`(?prod.cat (eq .name $strA))`, `{id:1 name:'a'}`},
		{`(?prod.cat _ id;)`, `{id:25}`},
		{`(?prod.cat _:id)`, `25`},
		{`(?prod.cat off:1)`, `{id:2 name:'b'}`},
		{`(?prod.cat off:$int1)`, `{id:2 name:'b'}`},
		{`(*prod.cat lim:2)`, `[{id:25 name:'y'} {id:2 name:'b'}]`},
		{`(*prod.cat asc:name off:1 lim:2)`, `[{id:2 name:'b'} {id:3 name:'c'}]`},
		{`(*prod.cat desc:name lim:2)`, `[{id:26 name:'z'} {id:25 name:'y'}]`},
		{`(?prod.label _ id; label:('Label: ' .name))`, `{id:1 label:'Label: M'}`},
		{`(*prod.label off:1 lim:2 - tmpl;)`, `[{id:2 name:'N'} {id:3 name:'O'}]`},
		{`(*prod.prod desc:cat asc:name lim:3)`,
			`[{id:1 name:'A' cat:3} {id:3 name:'C' cat:3} {id:2 name:'B' cat:2}]`},
		{`(?prod.cat (eq .name 'c') +
			prods:(*prod.prod (eq .cat ..id) asc:name _ id; name;)
		)`, `{id:3 name:'c' prods:[{id:1 name:'A'} {id:3 name:'C'}]}`},
		{`(*prod.cat (or (eq .name 'b') (eq .name 'c')) asc:name +
			prods:(*prod.prod (eq .cat ..id) asc:name _ id; name;)
		)`, `[{id:2 name:'b' prods:[{id:2 name:'B'} {id:4 name:'D'}]} ` +
			`{id:3 name:'c' prods:[{id:1 name:'A'} {id:3 name:'C'}]}]`},
		{`(*prod.prod (ge .id 25) + catn:(?prod.cat (eq .id ..cat) _:name))`,
			`[{id:25 name:'Y' cat:1 catn:'a'} {id:26 name:'Z' cat:1 catn:'a'}]`},
	}
	param := &lit.Dict{Keyed: []lit.KeyVal{
		{Key: "int1", Val: lit.Int(1)},
		{Key: "strA", Val: lit.Str("a")},
		{Key: "list", Val: &lit.List{Reg: reg, El: typ.Str, Vals: []lit.Val{
			lit.Str("a"),
			lit.Str("b"),
			lit.Str("c"),
		}}},
	}}
	qry := qry.New(reg, extlib.Std, b)
	for _, test := range tests {
		start := time.Now()
		el, err := qry.Exec(test.Raw, param)
		end := time.Now()
		if err != nil {
			t.Errorf("qry %s error %+v", test.Raw, err)
			continue
		}
		if el == nil {
			t.Errorf("qry %s got nil result", test.Raw)
			continue
		}
		if got := bfr.String(el); got != test.Want {
			t.Errorf("want for %s\n\t%s got %s", test.Raw, test.Want, got)
			continue
		}
		log.Printf("test took %s", end.Sub(start))
	}
}
