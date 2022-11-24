package evtpgx

import (
	"context"
	"fmt"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/dapgx/qrypgx"
	"xelf.org/daql/dom"
	"xelf.org/daql/evt"
	"xelf.org/xelf/lit"
)

func NewLedger(db *pgxpool.Pool, pr *dom.Project, reg *lit.Regs) (evt.Ledger, error) {
	l, err := newLedger(db, pr, reg)
	if err != nil {
		return nil, err
	}
	return &l, nil
}

type ledger struct {
	*qrypgx.Backend
	Reg lit.Regs
	rev time.Time
}

func newLedger(db *pgxpool.Pool, pr *dom.Project, reg *lit.Regs) (ledger, error) {
	rev, err := queryMaxRev(db)
	return ledger{qrypgx.New(db, pr), *lit.DefaultRegs(reg), rev}, err
}

func (l *ledger) Rev() time.Time        { return l.rev }
func (l *ledger) Project() *dom.Project { return l.Backend.Project }

func (l *ledger) Events(ctx context.Context, rev time.Time, tops ...string) ([]*evt.Event, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if rev.IsZero() && len(tops) == 0 {
		return l.queryEvents(ctx, l.DB, "")
	}
	if len(tops) == 0 {
		return l.queryEvents(ctx, l.DB, "WHERE rev > $1", rev)
	}
	return l.queryEvents(ctx, l.DB, "WHERE rev > $1 AND top in $2", rev, tops)
}

func queryMaxRev(c dapgx.C) (time.Time, error) {
	rev := time.Time{}
	err := c.QueryRow(context.Background(),
		"SELECT rev FROM evt.event ORDER BY rev DESC LIMIT 1").Scan(&rev)
	if err != nil && err != pgx.ErrNoRows {
		return rev, err
	}
	return rev, nil
}
func (l *ledger) queryEvents(ctx context.Context, c dapgx.C, whr string, args ...interface{}) (res []*evt.Event, _ error) {
	rows, err := c.Query(ctx, fmt.Sprintf("SELECT id, rev, top, key, cmd, arg "+
		"FROM evt.event %s ORDER BY id", whr), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	err = dapgx.ScanMany(l.Reg, false, lit.MustProxy(l.Reg, &res), rows)
	if err != nil {
		return nil, err
	}
	return res, nil
}
