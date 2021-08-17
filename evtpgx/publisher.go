package evtpgx

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/daql/dom"
	"xelf.org/daql/evt"
	"xelf.org/xelf/lit"
)

type applyFunc func(*publisher, dapgx.C, []*evt.Event) error

type Publisher struct {
	publisher
}

type publisher struct {
	ledger
	insTop map[string]string
	apply  applyFunc
}

func NewPublisher(db *pgxpool.Pool, pr *dom.Project, reg *lit.Reg) (*Publisher, error) {
	return newPublisher(db, pr, reg, insertEvents)
}
func NewStatefulPublisher(db *pgxpool.Pool, pr *dom.Project, reg *lit.Reg) (*Publisher, error) {
	return newPublisher(db, pr, reg, applyAndInsertEvents)
}
func newPublisher(db *pgxpool.Pool, pr *dom.Project, reg *lit.Reg, a applyFunc) (*Publisher, error) {
	l, err := newLedger(db, pr, reg)
	if err != nil {
		return nil, err
	}
	return &Publisher{publisher{ledger: l, apply: a}}, nil
}

func (p *Publisher) Publish(t evt.Trans) (time.Time, []*evt.Event, error) {
	if t.Base.IsZero() {
		t.Base = p.rev
	} else if t.Base.After(p.rev) {
		return p.rev, nil, fmt.Errorf("sanity check publish future base revision")
	}
	now := time.Now()
	if t.Arrived.IsZero() {
		t.Arrived = now
	}
	if t.Created.IsZero() {
		t.Created = now
	}
	evs := make([]*evt.Event, 0, len(t.Acts))
	rev := evt.NextRev(p.rev, now)
	check := p.rev.After(t.Base)
	var keys []string
	for _, act := range t.Acts {
		if check && act.Cmd != evt.CmdNew {
			// collect the keys to look for conflicts
			keys = append(keys, act.Key)
		}
		evs = append(evs, &evt.Event{Rev: rev, Action: act})
	}
	err := dapgx.WithTx(p.DB, func(c dapgx.C) error {
		cur, err := queryMaxRev(c)
		if err != nil {
			return fmt.Errorf("query max rev: %w", err)
		}
		if !cur.Equal(p.rev) {
			return fmt.Errorf("sanity check publish ledger revision out of sync")
		}
		if check && len(keys) > 0 {
			// query events with affected keys since base revision
			diff, err := p.queryEvents(c, "WHERE rev > $1 AND key IN $2", t.Base, keys)
			if err != nil {
				return err
			}
			if len(diff) > 0 {
				// TODO check for conflict
			}
		}
		// insert audit
		err = p.apply(&p.publisher, c, evs)
		if err != nil {
			log.Printf("apply failed %v", err)
			return err
		}
		err = p.insertAudit(c, rev, t.Audit)
		if err != nil {
			log.Printf("insert audit failed %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		return p.rev, nil, err
	}
	p.rev = rev
	return p.rev, evs, nil
}

func (p *publisher) insertAudit(c dapgx.C, rev time.Time, d evt.Audit) error {
	_, err := c.Exec(dapgx.BG, `INSERT INTO evt.audit
		(rev, created, arrived, usr, extra) VALUES
		($1, $2, $3, $4, $5)`,
		rev, d.Created, d.Arrived, d.Usr, p.arg(d.Extra),
	)
	if err != nil {
		return fmt.Errorf("insert audit: %w", err)
	}
	return nil
}

func insertEvents(p *publisher, c dapgx.C, evs []*evt.Event) error {
	for _, ev := range evs {
		err := c.QueryRow(dapgx.BG, `INSERT INTO evt.event
			(rev, top, key, cmd, arg) VALUES
			($1, $2, $3, $4, $5) RETURNING id`,
			ev.Rev, ev.Top, ev.Key, ev.Cmd, p.arg(ev.Arg),
		).Scan(&ev.ID)
		if err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
	}
	return nil
}

func applyAndInsertEvents(p *publisher, c dapgx.C, evs []*evt.Event) error {
	for _, ev := range evs {
		err := applyEvent(p, c, ev)
		if err != nil {
			return fmt.Errorf("apply event: %w", err)
		}
	}
	return insertEvents(p, c, evs)
}

func applyEvent(p *publisher, c dapgx.C, ev *evt.Event) (err error) {
	m := p.Project().Model(ev.Top)
	if m == nil {
		return fmt.Errorf("no model found for topic %s", ev.Top)
	}
	switch ev.Cmd {
	case evt.CmdDel:
		stmt := fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.Qualified())
		_, err = c.Exec(dapgx.BG, stmt, ev.Key)
	case evt.CmdNew:
		qry := p.insTop[ev.Top]
		if qry == "" {
			qry = insertObj(m)
			if p.insTop == nil {
				p.insTop = make(map[string]string)
			}
			p.insTop[ev.Top] = qry
		}
		args, err := p.insertArgs(m, ev)
		if err != nil {
			return err
		}
		_, err = c.Exec(dapgx.BG, qry, args...)
	case evt.CmdMod:
		qry, args, err := p.updateObj(m, ev)
		if err != nil {
			return err
		}
		_, err = c.Exec(dapgx.BG, qry, args...)
	default:
	}
	if err != nil {
		return fmt.Errorf("apply %s %s %s %s: %w", ev.Top, ev.Key, ev.Cmd, ev.Arg, err)
	}
	return nil
}

func insertObj(m *dom.Model) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(m.Qualified())
	b.WriteString(" (")
	for i, f := range m.Elems {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(f.Key())
	}
	b.WriteString(") VALUES (")
	for i := range m.Elems {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("$%d", i+1))
	}
	b.WriteString(")")
	return b.String()
}

func (p *publisher) insertArgs(m *dom.Model, ev *evt.Event) ([]interface{}, error) {
	args := make([]interface{}, 0, len(m.Elems))
	for _, f := range m.Elems {
		k := f.Key()
		if k == "id" {
			args = append(args, ev.Key)
		} else if k == "rev" {
			args = append(args, ev.Rev)
		} else {
			v, err := ev.Arg.Key(k)
			if err != nil {
				return nil, err
			}
			args = append(args, p.arg(v))
		}
	}
	return args, nil
}

func (p *publisher) updateObj(m *dom.Model, ev *evt.Event) (string, []interface{}, error) {
	args := make([]interface{}, 0, ev.Arg.Len()+1)
	args = append(args, ev.Key)
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(m.Qualified())
	b.WriteString(" SET ")
	for _, f := range m.Elems {
		k := f.Key()
		if k == "id" {
			continue
		}
		var arg interface{}
		if k == "rev" {
			arg = ev.Rev
		} else {
			val, err := ev.Arg.Key(k)
			if err != nil {
				return "", nil, err
			}
			if val.Zero() {
				continue
			}
			arg = p.arg(val)
		}
		if len(args) > 1 {
			b.WriteString(", ")
		}
		args = append(args, arg)
		b.WriteString(k)
		b.WriteString(" = ")
		b.WriteString(fmt.Sprintf("$%d", len(args)))
	}
	b.WriteString(" WHERE id = $1")
	return b.String(), args, nil
}
