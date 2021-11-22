package dompgx

import (
	"strings"
	"testing"

	"xelf.org/dapgx"
	"xelf.org/daql/dom"
	"xelf.org/xelf/bfr"
	"xelf.org/xelf/lit"
)

const fooRaw = `(schema foo
	(Align:bits A; B; C:3)
	(Kind:enum A; B; C;)
	(Node1; (Name?:str idx;))
	(Node2; (ID:int pk;) (Start:time uniq;))
	(Node3; (ID:int pk;) Group:str Name:str uniq:['group' 'name'])
	(Node4; (ID:int pk;) @Node2.ID)
	(Node5; (ID:int pk;) (Val:bool def:false))
)`

func TestWriteTable(t *testing.T) {
	reg := &lit.Reg{}
	s, err := dom.ReadSchema(reg, strings.NewReader(fooRaw), "foo", nil)
	if err != nil {
		t.Fatalf("schema foo error %v", err)
	}
	tests := []struct {
		model string
		want  string
	}{
		{"kind", "CREATE TYPE foo.kind AS ENUM (\n\t'a',\n\t'b',\n\t'c'\n);"},
		{"node1", "CREATE TABLE foo.node1 (\n\tname text NULL\n);\n" +
			"CREATE INDEX node1_name_idx on foo.node1 (name);"},
		{"node2", "CREATE TABLE foo.node2 (\n\tid int8 PRIMARY KEY,\n\tstart timestamptz NOT NULL UNIQUE\n);"},
		{"node3", "CREATE TABLE foo.node3 (\n\tid int8 PRIMARY KEY,\n" +
			"\t\"group\" text NOT NULL,\n\tname text NOT NULL\n);\n" +
			"CREATE UNIQUE INDEX node3_group_name_uniq on foo.node3 (group, name);"},
		{"node4", "CREATE TABLE foo.node4 (\n\tid int8 PRIMARY KEY,\n" +
			"\tnode2 int8 NOT NULL REFERENCES foo.node2 deferrable\n);"},
		{"node5", "CREATE TABLE foo.node5 (\n\tid int8 PRIMARY KEY,\n" +
			"\tval bool NOT NULL DEFAULT false\n);"},
	}
	for _, test := range tests {
		var b strings.Builder
		w := dapgx.NewWriter(bfr.P{Writer: &b},
			&dom.Project{Name: "test", Schemas: []*dom.Schema{s}}, nil, nil)
		m := s.Model(test.model)
		if m == nil {
			t.Errorf("model %s not found", test.model)
		}
		err := WriteModel(w, m)
		if err != nil {
			t.Errorf("write model %s err %v", test.model, err)
		}
		if got := b.String(); got != test.want {
			t.Errorf("model %s\n  got: %s\n want: %s", test.model, got, test.want)
		}
	}
}
