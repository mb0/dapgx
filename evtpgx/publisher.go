package evtpgx

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/dapgx"
	"xelf.org/daql/dom"
	"xelf.org/daql/evt"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
)

type applyFunc func(context.Context, *publisher, dapgx.PC, []*evt.Event) error

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
	check := false // p.rev.After(t.Base)
	var keys []string
	for _, act := range t.Acts {
		if check && act.Cmd != evt.CmdNew {
			// collect the keys to look for conflicts
			keys = append(keys, act.Key)
		}
		evs = append(evs, &evt.Event{Rev: rev, Action: act})
	}
	ctx := context.Background()
	err := dapgx.WithTx(ctx, p.DB, func(c dapgx.PC) error {
		cur, err := queryMaxRev(c)
		if err != nil {
			return fmt.Errorf("query max rev: %w", err)
		}
		if !cur.Equal(p.rev) {
			return fmt.Errorf("sanity check publish ledger revision out of sync")
		}
		if check && len(keys) > 0 {
			// query events with affected keys since base revision
			diff, err := p.queryEvents(ctx, c, "WHERE rev > $1 AND key IN $2", t.Base, keys)
			if err != nil {
				return err
			}
			if len(diff) > 0 {
				// TODO check for conflict
			}
		}
		err = p.apply(ctx, &p.publisher, c, evs)
		if err != nil {
			log.Printf("apply failed %v", err)
			return err
		}
		// insert audit
		err = p.insertAudit(ctx, c, rev, t.Audit)
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

func (p *publisher) insertAudit(ctx context.Context, c dapgx.PC, rev time.Time, d evt.Audit) error {
	err := dapgx.Exec(ctx, c, `INSERT INTO evt.audit
		(rev, created, arrived, usr, extra) VALUES
		($1, $2, $3, $4, $5)`, []lit.Val{
		lit.Time(rev), lit.Time(d.Created), lit.Time(d.Arrived),
		lit.Str(d.Usr), d.Extra,
	})
	if err != nil {
		return fmt.Errorf("insert audit: %w", err)
	}
	return nil
}

func scanOne(rows pgx.Rows, arg ...interface{}) error {
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("no rows returned")
	}
	err := rows.Scan(arg...)
	if err != nil {
		return err
	}
	return rows.Err()
}

func insertEvents(ctx context.Context, p *publisher, c dapgx.PC, evs []*evt.Event) error {
	for _, ev := range evs {
		rows, err := dapgx.Query(ctx, c, `INSERT INTO evt.event
			(rev, top, key, cmd, arg) VALUES
			($1, $2, $3, $4, $5) returning id`, []lit.Val{
			lit.Time(ev.Rev), lit.Str(ev.Top), lit.Str(ev.Key), lit.Str(ev.Cmd), ev.Arg,
		})
		if err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
		err = scanOne(rows, &ev.ID)
		if err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
	}
	return nil
}

func applyAndInsertEvents(ctx context.Context, p *publisher, c dapgx.PC, evs []*evt.Event) error {
	for _, ev := range evs {
		err := applyEvent(ctx, p, c, ev)
		if err != nil {
			return fmt.Errorf("apply event: %w", err)
		}
	}
	return insertEvents(ctx, p, c, evs)
}

func applyEvent(ctx context.Context, p *publisher, c dapgx.PC, ev *evt.Event) error {
	m := p.Project().Model(ev.Top)
	if m == nil {
		return fmt.Errorf("no model found for topic %s", ev.Top)
	}
	switch ev.Cmd {
	case evt.CmdDel:
		stmt := fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.Qualified())
		_, err := c.Exec(ctx, stmt, ev.Key)
		if err != nil {
			return err
		}
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
		err = dapgx.Exec(ctx, c, qry, args)
		if err != nil {
			return err
		}
	case evt.CmdMod:
		qry, args, err := p.updateObj(m, ev)
		if err != nil {
			return err
		}
		err = dapgx.Exec(ctx, c, qry, args)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected command %s", ev.Cmd)
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

func keyToID(f *dom.Elem, key string) (lit.Val, error) {
	switch f.Type.Kind & knd.Prim {
	case knd.Int:
		n, err := strconv.ParseInt(key, 10, 64)
		return lit.Int(n), err
	case knd.Str:
		return lit.Str(key), nil
	case knd.UUID:
		u, err := cor.ParseUUID(key)
		return lit.UUID(u), err
	case knd.Time:
		t, err := cor.ParseTime(key)
		return lit.Time(t), err
	}
	return nil, fmt.Errorf("unexpected id type %s", f.Type)
}

func (p *publisher) insertArgs(m *dom.Model, ev *evt.Event) ([]lit.Val, error) {
	args := make([]lit.Val, 0, len(m.Elems))
	for _, f := range m.Elems {
		k := f.Key()
		if k == "id" {
			id, err := keyToID(f, ev.Key)
			if err != nil {
				return nil, err
			}
			args = append(args, id)
		} else if k == "rev" {
			args = append(args, lit.Time(ev.Rev))
		} else {
			v, err := ev.Arg.Key(k)
			if err != nil {
				return nil, err
			}
			args = append(args, v)
		}
	}
	return args, nil
}

func (p *publisher) updateObj(m *dom.Model, ev *evt.Event) (string, []lit.Val, error) {
	args := make([]lit.Val, 0, ev.Arg.Len()+1)
	args = append(args, lit.Str(ev.Key))
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(m.Qualified())
	b.WriteString(" SET ")
	for _, f := range m.Elems {
		k := f.Key()
		if k == "id" {
			continue
		}
		var arg lit.Val
		if k == "rev" {
			arg = lit.Time(ev.Rev)
		} else {
			val, err := ev.Arg.Key(k)
			if err != nil {
				return "", nil, err
			}
			if val.Zero() {
				continue
			}
			arg = val
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
