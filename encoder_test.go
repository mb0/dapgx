package dapgx

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"xelf.org/xelf/lit"
)

type timeTest struct {
	oid  uint32
	time time.Time
	want time.Time
}

func TestTime(t *testing.T) {
	same := func(oid uint32, t time.Time) timeTest {
		return timeTest{oid, t, t}
	}
	tests := []timeTest{
		same(pgtype.TimestampOID, time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC)),
		same(pgtype.TimestamptzOID, time.Date(1800, 1, 1, 23, 59, 0, 0, time.UTC)),
		{pgtype.DateOID, time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1800, 1, 1, 0, 0, 0, 0, time.Local)},
		same(pgtype.TimestampOID, time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local)),
		same(pgtype.TimestamptzOID, time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local)),
		same(pgtype.DateOID, time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local)),
	}
	reg := &lit.Reg{Cache: &lit.Cache{}}
	for _, test := range tests {
		enc, err := FieldEncoder(test.oid, lit.Time(test.time))
		if err != nil {
			t.Errorf("no encoder %v", err)
			continue
		}
		txt, err := enc.EncodeText(nil, nil)
		if err != nil {
			t.Errorf("encode text %v", err)
			continue
		}
		bin, err := enc.EncodeBinary(nil, nil)
		if err != nil {
			t.Errorf("encode binary %v", err)
			continue
		}
		decs := FieldDecoders(test.oid)
		tv, err := decs.Text(txt, reg)
		if err != nil {
			t.Errorf("decode text %v", err)
			continue
		}
		bv, err := decs.Binary(bin, reg)
		if err != nil {
			t.Errorf("decode binary %v", err)
			continue
		}
		if !lit.Equal(lit.Time(test.want), tv) {
			t.Errorf("text time does not match %s != %s", test.want, tv)
		}
		if !lit.Equal(lit.Time(test.want), bv) {
			t.Errorf("binary time does not match %s != %s", test.want, bv)
		}
	}
}
