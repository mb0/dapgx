package evtpgx

import (
	"testing"
	"time"

	"xelf.org/daql/evt"
	"xelf.org/xelf/lit"
)

func TestReclicator(t *testing.T) {
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
		{Sig: evt.Sig{"prod.cat", "1"}, Cmd: evt.CmdNew, Arg: &lit.Dict{Keyed: []lit.KeyVal{
			{Key: "name", Val: lit.Str("a")},
		}}},
		{Sig: evt.Sig{"prod.prod", "25"}, Cmd: evt.CmdNew, Arg: &lit.Dict{Keyed: []lit.KeyVal{
			{Key: "name", Val: lit.Str("Y")},
			{Key: "cat", Val: lit.Int(1)},
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
	evs, err = r.Events(time.Time{})
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
	catn, err := queryCount(db, "prod.cat")
	if err != nil || catn != 1 {
		t.Errorf("want 1 cats got %d %v", catn, err)
	}
	prodn, err := queryCount(db, "prod.prod")
	if err != nil || prodn != 1 {
		t.Errorf("want 1 prods got %d %v", prodn, err)
	}
}
