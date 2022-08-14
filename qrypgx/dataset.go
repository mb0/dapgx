package qrypgx

import (
	"context"
	"fmt"
	"io"
	"strings"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/daql/dom"
	"xelf.org/daql/mig"
	"xelf.org/xelf/lit"
)

var _ mig.Dataset = (*Backend)(nil)

func (b *Backend) Vers() *mig.Version { return b.Version }

// Close satisfies the dataset interface but does not close the underlying connection pool.
func (b *Backend) Close() error { return nil }

func (b *Backend) Keys() []string {
	res := make([]string, 0, len(b.tables))
	for k := range b.tables {
		res = append(res, k)
	}
	return res
}

func (b *Backend) Stream(key string) (mig.Stream, error) {
	m := b.tables[key]
	if m != nil {
		return openRowsIter(b.DB, m)
	}
	return nil, fmt.Errorf("no table with key %s", key)
}

func openRowsIter(db *pgxpool.Pool, m *dom.Model) (*rowsIter, error) {
	res, err := lit.NewObj(&lit.Reg{}, m.Type())
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, k := range res.Keys() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k)
	}
	b.WriteString(" FROM ")
	b.WriteString(m.Qualified())
	ctx := context.Background()
	rows, err := db.Query(ctx, b.String())
	if err != nil {
		return nil, err
	}
	return &rowsIter{rows, res, nil}, err
}

type rowsIter struct {
	pgx.Rows
	res  *lit.Obj
	args []interface{}
}

func (it *rowsIter) Scan() (lit.Val, error) {
	if !it.Next() {
		return nil, io.EOF
	}
	val, _ := it.res.New()
	res := val.(*lit.Obj)
	if it.args == nil {
		it.args = make([]interface{}, len(res.Vals))
	}
	args := it.args[:]
	for _, v := range res.Vals {
		args = append(args, v.(lit.Mut).Ptr())
	}
	err := it.Rows.Scan(args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (it *rowsIter) Close() error { it.Rows.Close(); return nil }
