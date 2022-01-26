package evtpgx

import (
	"context"
	"testing"
	"time"

	"xelf.org/daql/evt"
	"xelf.org/xelf/lit"
)

var _ evt.Replicator = (*Replicator)(nil)

func TestReplicator(t *testing.T) {
	reg, pr, db := testSetup(t)
	defer db.Close()
	p, err := NewStatefulPublisher(db, pr, reg)
	if err != nil {
		t.Fatalf("create publisher %v", err)
	}
	r, err := NewReplicator(p)
	if err != nil {
		t.Fatalf("create replicator %v", err)
	}
	rev, evs, err := r.PublishLocal(evt.Trans{Acts: []evt.Action{
		{Sig: evt.Sig{"person.group", "1"}, Cmd: evt.CmdNew, Arg: &lit.Dict{Keyed: []lit.KeyVal{
			{Key: "name", Val: lit.Str("Test")},
		}}},
		{Sig: evt.Sig{"person.person", "25"}, Cmd: evt.CmdNew, Arg: &lit.Dict{Keyed: []lit.KeyVal{
			{Key: "name", Val: lit.Str("Me")},
			{Key: "family", Val: lit.Int(1)},
		}}},
	}})
	if err != nil {
		t.Fatalf("first %v", err)
	}
	if rev.IsZero() {
		t.Fatalf("publ lrev is zero")
	}
	if !r.LocalRev().Equal(rev) {
		t.Fatalf("publ lrev is not equal replicator lrev")
	}
	ctx := context.Background()
	evs, err = r.Events(ctx, time.Time{})
	if err != nil {
		t.Fatalf("lpub events %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("lpub events want 0 got %d", len(evs))
	}
	locs := r.Locals()
	if len(locs) != 1 || len(locs[0].Acts) != 2 {
		t.Fatalf("pub local actions want 2 got %d", len(locs[0].Acts))
	}
	groupn, err := queryCount(db, "person.group")
	if err != nil || groupn != 1 {
		t.Errorf("want 1 groups got %d %v", groupn, err)
	}
	persn, err := queryCount(db, "person.person")
	if err != nil || persn != 1 {
		t.Errorf("want 1 persons got %d %v", persn, err)
	}
}
