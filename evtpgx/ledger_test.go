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

func testSetup(t *testing.T) (*lit.Regs, *dom.Project, *pgxpool.Pool) {
	t.Helper()
	reg := lit.NewRegs()
	pr, err := testProject()
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

func testProject() (*dom.Project, error) {
	mig, err := dom.ReadSchema(nil, strings.NewReader(mig.RawSchema()), "mig.daql")
	if err != nil {
		return nil, err
	}
	ev, err := dom.ReadSchema(nil, strings.NewReader(evt.RawSchema()), "evt.daql")
	if err != nil {
		return nil, err
	}
	pr, err := dom.ReadSchema(nil, strings.NewReader(domtest.PersonRaw), "person.daql")
	if err != nil {
		return nil, err
	}
	p := &dom.Project{}
	p.Schemas = append(p.Schemas, mig, ev, pr)
	return p, nil
}

func queryCount(c dapgx.C, table string) (res int, err error) {
	err = c.QueryRow(context.Background(), fmt.Sprintf(`select count(*) from %s`, table)).Scan(&res)
	return res, err
}
