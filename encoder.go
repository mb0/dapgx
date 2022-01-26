// The individual encoder method details were mostly rewritten based on pgtype.
// pgtype uses the MIT LICENSE and the Copyright (c) 2013-2021 Jack Christensen

package dapgx

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
)

func FieldEncoder(oid uint32, arg lit.Val) (encoder, error) {
	if arg == nil || arg.Nil() {
		return WrapNull{}, nil
	}
	if oid > pgtype.Int8rangeOID { // this is the max common oid pgtype knows about
		// we may have an enum so lets use the arg type as hint
		k := arg.Type().Kind
		switch {
		case k&knd.Char != 0:
			return WrapStr(arg.String()), nil
		case k&knd.Int != 0:
			a, err := lit.ToInt(arg)
			return WrapInt8(a), err
		}
	}
	switch oid {
	case pgtype.BoolOID:
		return WrapBool(!arg.Zero()), nil
	case pgtype.ByteaOID:
		a, err := lit.ToRaw(arg)
		return WrapRaw(a), err
	case pgtype.Int2OID:
		a, err := lit.ToInt(arg)
		return WrapInt2(a), err
	case pgtype.Int4OID:
		a, err := lit.ToInt(arg)
		return WrapInt4(a), err
	case pgtype.Int8OID:
		a, err := lit.ToInt(arg)
		return WrapInt8(a), err
	case pgtype.Float4OID:
		a, err := lit.ToReal(arg)
		return WrapReal4(a), err
	case pgtype.Float8OID:
		a, err := lit.ToReal(arg)
		return WrapReal8(a), err
	case pgtype.TextOID, pgtype.VarcharOID:
		return WrapStr(arg.String()), nil
	case pgtype.UUIDOID:
		a, err := lit.ToUUID(arg)
		return WrapUUID(a), err
	case pgtype.TimestamptzOID:
		a, err := lit.ToTime(arg)
		return WrapTime(a), err
	case pgtype.DateOID:
		a, err := lit.ToTime(arg)
		return WrapTimeDate(a), err
	case pgtype.TimestampOID:
		a, err := lit.ToTime(arg)
		return WrapTimestamp(a), err
	case pgtype.TimeOID:
		a, err := lit.ToSpan(arg)
		return WrapSpanTime(a), err
	case pgtype.IntervalOID:
		a, err := lit.ToSpan(arg)
		return WrapSpan(a), err
	case pgtype.JSONOID:
		return WrapJSON{arg}, nil
	case pgtype.JSONBOID:
		return WrapJSONB{arg}, nil
	}
	if idxr, ok := arg.(lit.Idxr); ok {
		var coid int32
		switch oid {
		case pgtype.BoolArrayOID:
			coid = pgtype.BoolOID
		case pgtype.ByteaArrayOID:
			coid = pgtype.ByteaOID
		case pgtype.Int2ArrayOID:
			coid = pgtype.Int2OID
		case pgtype.Int4ArrayOID:
			coid = pgtype.Int4OID
		case pgtype.Int8ArrayOID:
			coid = pgtype.Int8OID
		case pgtype.Float4ArrayOID:
			coid = pgtype.Float4OID
		case pgtype.Float8ArrayOID:
			coid = pgtype.Float8OID
		case pgtype.TextArrayOID, pgtype.VarcharArrayOID:
			coid = pgtype.TextOID
		case pgtype.UUIDArrayOID:
			coid = pgtype.UUIDOID
		case pgtype.DateArrayOID:
			coid = pgtype.DateOID
		case pgtype.TimestampArrayOID:
			coid = pgtype.TimestampOID
		case pgtype.TimestamptzArrayOID:
			coid = pgtype.TimestamptzOID
		case 1183 /*TimeArrayOID*/ :
			coid = pgtype.TimeOID
		case 1187 /*IntervalArrayOID*/ :
			coid = pgtype.IntervalOID
		case 199 /* JSONArrayOID*/ :
			coid = pgtype.JSONOID
		case pgtype.JSONBArrayOID:
			coid = pgtype.JSONBOID
		default:
			return nil, fmt.Errorf("no array encoder for %T", arg)
		}
		return WrapIdxr{idxr, coid}, nil
	}
	return nil, fmt.Errorf("no encoder for %T", arg)
}

func isSpace(b byte) bool { return b == ' ' || b > '\t' && b < '\r' }

func quote(oid int32, raw []byte) []byte {
	switch oid {
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.JSONOID, pgtype.JSONBOID:
	default:
		return raw
	}
	if len(raw) == 0 || isSpace(raw[0]) || isSpace(raw[len(raw)-1]) ||
		bytes.ContainsAny(raw, `{},"\`) || bytes.EqualFold(raw, []byte("null")) {
		res := make([]byte, 0, len(raw)+8)
		res = append(res, '"')
		for i := 0; i < len(raw); {
			r, n := utf8.DecodeRune(raw[i:])
			if n <= 0 {
				break
			}
			switch r {
			case '\\', '"':
				res = append(res, '\\', raw[i])
			default:
				res = append(res, raw[i:n]...)
			}
			i += n
		}
		res = append(res, '"')
	}
	return raw
}

type encoder interface {
	pgtype.TextEncoder
	pgtype.BinaryEncoder
}

type (
	WrapNull      lit.Null
	WrapBool      lit.Bool
	WrapInt2      lit.Int
	WrapInt4      lit.Int
	WrapInt8      lit.Int
	WrapReal4     lit.Real
	WrapReal8     lit.Real
	WrapStr       lit.Str
	WrapRaw       lit.Raw
	WrapUUID      lit.UUID
	WrapTime      lit.Time
	WrapTimeDate  lit.Time
	WrapTimestamp lit.Time
	WrapSpan      lit.Span
	WrapSpanTime  lit.Span
	WrapJSON      struct{ lit.Val }
	WrapJSONB     struct{ lit.Val }
	WrapIdxr      struct {
		lit.Idxr
		Oid int32
	}
)

func (w WrapNull) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error)   { return nil, nil }
func (w WrapNull) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) { return nil, nil }

func (w WrapBool) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	if w {
		return append(b, 't'), nil
	}
	return append(b, 'f'), nil
}
func (w WrapBool) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	if w {
		return append(b, 1), nil
	}
	return append(b, 0), nil
}

func (w WrapInt2) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, strconv.FormatInt(int64(w), 10)...), nil
}
func (w WrapInt2) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return pgio.AppendInt16(b, int16(w)), nil
}
func (w WrapInt4) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, strconv.FormatInt(int64(w), 10)...), nil
}
func (w WrapInt4) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return pgio.AppendInt32(b, int32(w)), nil
}
func (w WrapInt8) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, strconv.FormatInt(int64(w), 10)...), nil
}
func (w WrapInt8) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return pgio.AppendInt64(b, int64(w)), nil
}

func (w WrapReal4) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, strconv.FormatFloat(float64(w), 'f', -1, 32)...), nil
}
func (w WrapReal4) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return pgio.AppendUint32(b, math.Float32bits(float32(w))), nil
}
func (w WrapReal8) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, strconv.FormatFloat(float64(w), 'f', -1, 64)...), nil
}
func (w WrapReal8) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return pgio.AppendUint64(b, math.Float64bits(float64(w))), nil
}

func (w WrapStr) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, string(w)...), nil
}
func (w WrapStr) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, string(w)...), nil
}

func (w WrapRaw) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(append(b, '\\', 'x'), hex.EncodeToString([]byte(w))...), nil
}
func (w WrapRaw) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, []byte(w)...), nil
}

func (w WrapUUID) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, cor.FormatUUID(w)...), nil
}
func (w WrapUUID) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	return append(b, w[:]...), nil
}

func (w WrapTime) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	s := time.Time(w).UTC().Format("2006-01-02 15:04:05.999999Z07:00:00")
	return append(b, s...), nil
}
func (w WrapTime) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	t := time.Time(w)
	µs := t.Unix()*1_000_000 + int64(t.Nanosecond())/1_000
	return pgio.AppendInt64(b, µs-sUnixToY2k*1_000_000), nil
}
func (w WrapTimeDate) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	s := time.Time(w).Format("2006-01-02")
	return append(b, s...), nil
}

var epochStamp = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

func (w WrapTimeDate) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	t := time.Time(w)
	s := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix()
	days := int32((s - epochStamp) / 86400)
	return pgio.AppendInt32(b, days), nil
}
func (w WrapTimestamp) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	s := time.Time(w).UTC().Format("2006-01-02 15:04:05.999999")
	return append(b, s...), nil
}
func (w WrapTimestamp) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	t := time.Time(w)
	µs := t.Unix()*1_000_000 + int64(t.Nanosecond())/1_000
	return pgio.AppendInt64(b, µs-sUnixToY2k*1_000_000), nil
}

const days = 24 * time.Hour

func (w WrapSpan) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	d := time.Duration(w)
	h := d / time.Hour
	m := (d % time.Hour) / time.Minute
	s := (d % time.Minute) / time.Second
	µ := (d % time.Second) / time.Microsecond
	return append(b, fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, µ)...), nil
}
func (w WrapSpan) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	d := time.Duration(w)
	b = pgio.AppendInt64(b, int64((d%days)/time.Microsecond))
	b = pgio.AppendInt32(b, int32(d/days))
	return append(b, 0, 0, 0, 0), nil
}

func (w WrapSpanTime) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	d := time.Duration(w)
	h := d / time.Hour
	m := (d % time.Hour) / time.Minute
	s := (d % time.Minute) / time.Second
	µ := (d % time.Second) / time.Microsecond
	return append(b, fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, µ)...), nil
}
func (w WrapSpanTime) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	d := time.Duration(w)
	return pgio.AppendInt64(b, int64(d/time.Microsecond)), nil
}

func (w WrapJSON) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	raw, err := w.MarshalJSON()
	return append(b, raw...), err
}
func (w WrapJSON) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	raw, err := w.MarshalJSON()
	return append(b, raw...), err
}
func (w WrapJSONB) EncodeText(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	raw, err := w.MarshalJSON()
	return append(b, raw...), err
}
func (w WrapJSONB) EncodeBinary(_ *pgtype.ConnInfo, b []byte) ([]byte, error) {
	raw, err := w.MarshalJSON()
	return append(append(b, 1), raw...), err
}

func (w WrapIdxr) EncodeText(ci *pgtype.ConnInfo, b []byte) ([]byte, error) {
	if w.Idxr == nil {
		return nil, nil
	}
	b = pgtype.EncodeTextArrayDimensions(b, []pgtype.ArrayDimension{
		{Length: int32(w.Len()), LowerBound: 1},
	})
	b = append(b, '{')
	err := w.IterIdx(func(idx int, v lit.Val) error {
		if idx > 0 {
			b = append(b, ',')
		}
		if v == nil || v.Nil() {
			b = append(b, "NULL"...)
			return nil
		}
		enc, err := FieldEncoder(uint32(w.Oid), v)
		if err != nil {
			return err
		}
		c, err := enc.EncodeText(ci, nil)
		if err != nil {
			return err
		}
		b = append(b, quote(w.Oid, c)...)
		return nil
	})
	return append(b, '}'), err
}
func (w WrapIdxr) EncodeBinary(ci *pgtype.ConnInfo, b []byte) ([]byte, error) {
	if w.Idxr == nil {
		return nil, nil
	}
	hdr := pgtype.ArrayHeader{ElementOID: w.Oid, Dimensions: []pgtype.ArrayDimension{
		{Length: int32(w.Len()), LowerBound: 1},
	}}
	w.IterIdx(func(idx int, v lit.Val) error {
		if v == nil || v.Nil() {
			hdr.ContainsNull = true
			return lit.BreakIter
		}
		return nil
	})
	b = hdr.EncodeBinary(ci, b)
	err := w.IterIdx(func(idx int, v lit.Val) error {
		if v == nil || v.Nil() {
			b = pgio.AppendInt32(b, -1)
			return nil
		}
		enc, err := FieldEncoder(uint32(w.Oid), v)
		if err != nil {
			return err
		}
		mark := len(b)
		b = append(b, 0, 0, 0, 0)
		b, err = enc.EncodeBinary(ci, b)
		if err != nil {
			return err
		}
		pgio.SetInt32(b[mark:], int32(len(b[mark+4:])))
		return nil
	})
	return b, err
}
