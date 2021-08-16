package evtpgx

import (
	"testing"
	"time"

	"xelf.org/daql/evt"
	"xelf.org/xelf/lit"
)

func TestPublisher(t *testing.T) {
	reg, pr, db := testSetup(t)
	defer db.Close()
	l, err := NewStatefulPublisher(db, pr, reg)
	if err != nil {
		t.Fatalf("create publisher %v", err)
	}
	if !l.Rev().IsZero() {
		t.Fatalf("initial rev is not zero")
	}
	evs, err := l.Events(time.Time{})
	if err != nil {
		t.Fatalf("initial events %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("initial events not empty")
	}
	rev, evs, err := l.Publish(evt.Trans{Acts: []evt.Action{
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
		t.Fatalf("pub rev is zero")
	}
	if !l.Rev().Equal(rev) {
		t.Fatalf("pub rev is not equal ledger rev")
	}
	evs, err = l.Events(time.Time{})
	if err != nil {
		t.Fatalf("pub events %v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("pub events want 2 got %d", len(evs))
	}
	if id := evs[0].ID; id != 1 {
		t.Errorf("pub events want id 1 got %d", id)
	}
	if id := evs[1].ID; id != 2 {
		t.Errorf("pub events want id 2 got %d", id)
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
