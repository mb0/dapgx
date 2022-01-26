package evtpgx

import (
	"testing"
	"time"

	"xelf.org/daql/evt"
	"xelf.org/xelf/lit"
)

var _ evt.Publisher = (*Publisher)(nil)

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
	evs, err := l.Events(nil, time.Time{})
	if err != nil {
		t.Fatalf("initial events %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("initial events not empty")
	}
	rev, evs, err := l.Publish(evt.Trans{Acts: []evt.Action{
		{Sig: evt.Sig{"person.group", "1"}, Cmd: evt.CmdNew, Arg: &lit.Dict{Keyed: []lit.KeyVal{
			{Key: "name", Val: lit.Str("Test")},
		}}},
		{Sig: evt.Sig{"person.person", "25"}, Cmd: evt.CmdNew, Arg: &lit.Dict{Keyed: []lit.KeyVal{
			{Key: "name", Val: lit.Str("Me")},
			{Key: "gender", Val: lit.Str("m")},
			{Key: "family", Val: lit.Int(1)},
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
	evs, err = l.Events(nil, time.Time{})
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
	groupn, err := queryCount(db, "person.group")
	if err != nil || groupn != 1 {
		t.Errorf("want 1 groups got %d %v", groupn, err)
	}
	persn, err := queryCount(db, "person.person")
	if err != nil || persn != 1 {
		t.Errorf("want 1 persons got %d %v", persn, err)
	}
}
