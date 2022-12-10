package qrypgx

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/daql/dom"
	"xelf.org/daql/qry"
	"xelf.org/xelf/mod"
)

var Prov = qry.Backends.Register(&provider{}, "postgres", "postgresql", "qrypgx")

type provider struct{ pools sync.Map }

func (p *provider) Provide(uri string, pro *dom.Project) (qry.Backend, error) {
	loc := mod.ParseLoc(uri)
	pguri := "postgres:" + loc.Path()
	db, err := p.pool(pguri)
	if err != nil {
		return nil, err
	}
	return New(db, pro), nil
}
func (p *provider) pool(uri string) (*pgxpool.Pool, error) {
	a, _ := p.pools.Load(uri)
	if a == nil {
		pool, err := pgxpool.Connect(context.Background(), uri)
		if err != nil {
			return nil, err
		}
		var loaded bool
		a, loaded = p.pools.LoadOrStore(uri, pool)
		if loaded {
			pool.Close()
		}
	}
	return a.(*pgxpool.Pool), nil
}
