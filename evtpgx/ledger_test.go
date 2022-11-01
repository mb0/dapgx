package evtpgx

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/dapgx/dompgx"
	"xelf.org/daql/dom"
	"xelf.org/daql/dom/domtest"
	"xelf.org/daql/evt"
	"xelf.org/daql/mig"
	"xelf.org/xelf/lit"
)

var testDsn = "host=/var/run/postgresql dbname=daql"

var _ evt.Ledger = (*ledger)(nil)

func testSetup(t *testing.T) (*lit.Reg, *dom.Project, *pgxpool.Pool) {
	t.Helper()
	reg := &lit.Reg{Cache: &lit.Cache{}}
	pr, err := testProject(reg)
	if err != nil {
		t.Fatalf("setup project %v", err)
	}
	ctx := context.Background()
	db, err := dapgx.Open(ctx, testDsn, nil)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if db != nil {
		err := dompgx.CreateProject(ctx, db, pr)
		if err != nil {
			db.Close()
			t.Fatalf("create project: %v", err)
		}
	}
	return reg, pr, db
}

func testProject(reg *lit.Reg) (*dom.Project, error) {
	p := &dom.Project{}
	mig, err := dom.ReadSchema(reg, strings.NewReader(mig.RawSchema()), "mig.daql", nil)
	if err != nil {
		return nil, err
	}
	ev, err := dom.ReadSchema(reg, strings.NewReader(evt.RawSchema()), "evt.daql", nil)
	if err != nil {
		return nil, err
	}
	pr, err := dom.ReadSchema(reg, strings.NewReader(domtest.PersonRaw), "person.daql", nil)
	if err != nil {
		return nil, err
	}
	p.Schemas = append(p.Schemas, mig, ev, pr)
	return p, nil
}

func queryCount(c dapgx.C, table string) (res int, err error) {
	err = c.QueryRow(context.Background(), fmt.Sprintf(`select count(*) from %s`, table)).Scan(&res)
	return res, err
}
