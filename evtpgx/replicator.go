package evtpgx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"xelf.org/dapgx"
	"xelf.org/daql/evt"
	"xelf.org/daql/log"
	"xelf.org/xelf/lit"
)

func Replay(p *Publisher, evs []*evt.Event) error {
	if len(evs) == 0 {
		return fmt.Errorf("no events")
	}
	r, err := NewReplicator(p)
	if err != nil {
		return err
	}
	// TODO check if events are sorted?
	if r.Rev().After(evs[0].Rev) {
		return fmt.Errorf("replay events before ledger rev")
	}
	return r.Replicate(evs[len(evs)-1].Rev, evs)
}

type Replicator struct {
	*publisher
	rels  []string
	local []*evt.Trans
	lrev  time.Time
}

func NewReplicator(p *Publisher, rels ...string) (evt.LocalPublisher, error) {
	local, err := p.queryLocal(context.Background(), p.DB)
	if err != nil {
		return nil, err
	}
	lrev := p.Rev()
	for _, loc := range local {
		if loc.Audit.Rev.After(lrev) {
			lrev = loc.Audit.Rev
		}
	}
	return &Replicator{publisher: &p.publisher, rels: rels, local: local, lrev: lrev}, nil
}

// Replicate will apply and save the given events and remove matching local transactions.
func (r *Replicator) Replicate(newrev time.Time, evs []*evt.Event) error {
	drop := r.checkLocal(evs)
	rev := r.rev
	ctx := context.Background()
	err := dapgx.WithTx(ctx, r.DB, func(c dapgx.PC) error {
		err := r.apply(ctx, r.publisher, c, evs)
		if err != nil {
			return err
		}
		// save newest revision
		for _, ev := range evs {
			if ev.Rev.After(rev) {
				rev = ev.Rev
			}
		}
		err = deleteLocal(ctx, c, drop)
		if err != nil {
			log.Error("evtpgx: local trans purge error", "err", err)
		}
		return err
	})
	if err == nil {
		// only update revision if the transaction was successful
		r.rev = rev
		if rev.After(r.lrev) {
			r.lrev = rev
		}
		r.dropLocal(drop)
	}
	return err
}

func (r *Replicator) LocalRev() time.Time { return r.lrev }
func (r *Replicator) PublishLocal(data evt.Trans) (lrev time.Time, evs []*evt.Event, err error) {
	t := &data
	t.Base = r.lrev
	ctx := context.Background()
	err = dapgx.WithTx(ctx, r.DB, func(c dapgx.PC) error {
		srev, err := queryMaxRev(c)
		if err != nil {
			return fmt.Errorf("query rev: %w", err)
		}
		if !srev.Equal(r.rev) {
			return fmt.Errorf("store rev out of sync got %s want %s", srev, r.rev)
		}
		// what next rev would be if published.
		// we use it on sync if there were no events in between.
		now := time.Now()
		if t.Created.IsZero() {
			t.Created = now
		}
		t.Rev = evt.NextRev(srev, now)
		evs, err = reviseActions(c, t)
		if err != nil {
			return err
		}
		// insert local
		rows, err := dapgx.Query(ctx, c, `INSERT INTO evt.trans
			(base, rev, created, arrived, usr, extra, acts)
			values ($1, $2, $3, $4, $5, $6, $7) returning id`, []lit.Val{
			lit.Time(t.Base),
			lit.Time(t.Rev),
			lit.Time(t.Created),
			lit.Time(t.Arrived),
			lit.Str(t.Usr),
			t.Extra,
			lit.MustProxy(r.Reg, &t.Acts),
		})
		if err != nil {
			return err
		}
		err = scanOne(rows, &t.ID)
		if err != nil {
			return err
		}
		// all went well. only apply the events
		for _, ev := range evs {
			err := applyEvent(ctx, r.publisher, c, ev)
			if err != nil {
				return fmt.Errorf("apply: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return r.lrev, nil, err
	}
	r.lrev = t.Rev
	r.local = append(r.local, t)
	return r.lrev, evs, nil
}

func (r *Replicator) Locals() []evt.Trans {
	res := make([]evt.Trans, 0, len(r.local))
	for i := range r.local {
		if tp := r.local[i]; tp != nil {
			res = append(res, *tp)
		}
	}
	return res
}

func reviseActions(c dapgx.C, t *evt.Trans) ([]*evt.Event, error) {
	evs := make([]*evt.Event, 0, len(t.Acts))
	for _, act := range t.Acts {
		evs = append(evs, &evt.Event{
			Rev:    t.Rev,
			Action: act,
		})
	}
	return evs, nil
}

func (p *publisher) queryLocal(ctx context.Context, c dapgx.C) ([]*evt.Trans, error) {
	rows, err := c.Query(ctx, `SELECT id, base, rev, created, arrived, usr, extra, acts
		FROM evt.trans ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []*evt.Trans
	mut, err := p.Reg.Proxy(&res)
	if err != nil {
		return nil, err
	}
	err = dapgx.ScanMany(p.Reg, false, mut, rows)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func deleteLocal(ctx context.Context, c dapgx.C, drop []int64) error {
	if len(drop) == 0 {
		return nil
	}
	var str strings.Builder
	str.WriteString(`DELETE FROM evt.trans WHERE id in (`)
	for i, id := range drop {
		if i > 0 {
			str.WriteString(", ")
		}
		fmt.Fprintf(&str, "%d", id)
	}
	str.WriteByte(')')
	_, err := c.Exec(ctx, str.String())
	return err
}
func (s *Replicator) checkLocal(evs []*evt.Event) (drop []int64) {
	if len(s.local) == 0 {
		return
	}
	// if we have local transaction we expect
	// the next replication to cover all of them
	// check anyway and at least log
	drop = make([]int64, 0, len(s.local))
	for _, t := range s.local {
		found := 0
		for _, act := range t.Acts {
			if coveredBy(t.Audit.Rev, act, evs) {
				found++
			}
		}
		if found > 0 || len(t.Acts) == 0 {
			drop = append(drop, t.ID)
		}
		if found < len(t.Acts) {
			log.Error("evpgx: local trans misses events", "trans", t)
		}
	}
	return
}
func coveredBy(rev time.Time, act evt.Action, evs []*evt.Event) bool {
	for _, e := range evs {
		if e.Top == act.Top && e.Key == act.Key && !rev.After(e.Rev) {
			return true
		}
	}
	return false
}
func (r *Replicator) dropLocal(drop []int64) {
	if len(drop) == 0 {
		return
	}
	res := r.local[:0]
Outer:
	for _, t := range r.local {
		for _, id := range drop {
			if id == t.ID {
				continue Outer
			}
		}
		res = append(res, t)
	}
	r.local = res
}
