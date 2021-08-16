package evtpgx

import (
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

func NewLedger(db *pgxpool.Pool, pr *dom.Project, reg *lit.Reg) (evt.Ledger, error) {
	l, err := newLedger(db, pr, reg)
	if err != nil {
		return nil, err
	}
	return &l, nil
}

type ledger struct {
	*qrypgx.Backend
	Reg *lit.Reg
	rev time.Time
}

func newLedger(db *pgxpool.Pool, pr *dom.Project, reg *lit.Reg) (ledger, error) {
	rev, err := queryMaxRev(db)
	return ledger{qrypgx.New(db, pr), reg, rev}, err
}

func (l *ledger) Rev() time.Time        { return l.rev }
func (l *ledger) Project() *dom.Project { return l.Backend.Project }

func (l *ledger) Events(rev time.Time, tops ...string) ([]*evt.Event, error) {
	if rev.IsZero() && len(tops) == 0 {
		return l.queryEvents(l.DB, "")
	}
	if len(tops) == 0 {
		return l.queryEvents(l.DB, "WHERE rev > $1", rev)
	}
	return l.queryEvents(l.DB, "WHERE rev > $1 AND top in $2", rev, tops)
}

func (l *ledger) arg(arg interface{}) interface{} { return dapgx.WrapArg(arg) }
func (l *ledger) ptr(arg interface{}) interface{} { return dapgx.WrapPtr(l.Reg, arg) }

func queryMaxRev(c dapgx.C) (time.Time, error) {
	rev := time.Time{}
	err := c.QueryRow(dapgx.BG, "SELECT rev FROM evt.event ORDER BY rev DESC LIMIT 1").Scan(&rev)
	if err != nil && err != pgx.ErrNoRows {
		return rev, err
	}
	return rev, nil
}
func (l *ledger) queryEvents(c dapgx.C, whr string, args ...interface{}) ([]*evt.Event, error) {
	rows, err := c.Query(dapgx.BG, fmt.Sprintf("SELECT id, rev, top, key, cmd, arg "+
		"FROM evt.event %s ORDER BY id", whr), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []*evt.Event
	for rows.Next() {
		var e evt.Event
		err = rows.Scan(&e.ID, &e.Rev, &e.Top, &e.Key, &e.Cmd, l.ptr(&e.Arg))
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		res = append(res, &e)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return res, nil
}
