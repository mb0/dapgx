// The individual deecoder method details were mostly rewritten based on pgtype.
// pgtype uses the MIT LICENSE and the Copyright (c) 2013-2021 Jack Christensen

package dapgx

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"xelf.org/xelf/cor"
	"xelf.org/xelf/knd"
	"xelf.org/xelf/lit"
	"xelf.org/xelf/typ"
)

// Decoder is a function to decode either a text or binary postgres result to a literal.
type Decoder func(raw []byte, reg *lit.Reg) (lit.Val, error)

// FieldDecoder returns a decoder for the given field description fd.
func FieldDecoder(oid uint32, bin bool) (res Decoder) {
	decs, ok := decmap[oid]
	if ok {
		if bin {
			res = decs.Binary
		} else {
			res = decs.Text
		}
	}
	if res == nil {
		res = errDecoder
	}
	return res
}

func FieldDecoders(oid uint32) DecoderPair {
	decs, ok := decmap[oid]
	if ok {
		return decs
	}
	return DecoderPair{errDecoder, errDecoder}
}

type DecoderPair struct{ Text, Binary Decoder }

var decmap = map[uint32]DecoderPair{
	pgtype.BoolOID:        {boolTextDec, boolBinDec},
	pgtype.ByteaOID:       {rawTextDec, rawBinDec},
	pgtype.Int2OID:        {intTextDec, int2BinDec},
	pgtype.Int4OID:        {intTextDec, int4BinDec},
	pgtype.Int8OID:        {intTextDec, int8BinDec},
	pgtype.Float4OID:      {realTextDec, real4BinDec},
	pgtype.Float8OID:      {realTextDec, real8BinDec},
	pgtype.TextOID:        {strDec, strDec},
	pgtype.VarcharOID:     {strDec, strDec},
	pgtype.UUIDOID:        {uuidTextDec, uuidBinDec},
	pgtype.DateOID:        {dateTextDec, dateBinDec},
	pgtype.TimestampOID:   {tsTextDec, tsBinDec},
	pgtype.TimestamptzOID: {tstzTextDec, tstzBinDec},
	pgtype.TimeOID:        {timeTextDec, timeBinDec},
	pgtype.IntervalOID:    {intervalTextDec, intervalBinDec},
	pgtype.JSONOID:        {jsonDec, jsonDec},
	pgtype.JSONBOID:       {jsonDec, jsonbDec},

	pgtype.BoolArrayOID:        arrayDecs(boolTextDec, boolBinDec, typ.Bool),
	pgtype.ByteaArrayOID:       arrayDecs(rawTextDec, rawBinDec, typ.Raw),
	pgtype.Int2ArrayOID:        arrayDecs(intTextDec, int2BinDec, typ.Int),
	pgtype.Int4ArrayOID:        arrayDecs(intTextDec, int4BinDec, typ.Int),
	pgtype.Int8ArrayOID:        arrayDecs(intTextDec, int8BinDec, typ.Int),
	pgtype.Float4ArrayOID:      arrayDecs(realTextDec, real4BinDec, typ.Real),
	pgtype.Float8ArrayOID:      arrayDecs(realTextDec, real8BinDec, typ.Real),
	pgtype.TextArrayOID:        arrayDecs(strDec, strDec, typ.Str),
	pgtype.VarcharArrayOID:     arrayDecs(strDec, strDec, typ.Str),
	pgtype.UUIDArrayOID:        arrayDecs(uuidTextDec, uuidBinDec, typ.UUID),
	pgtype.DateArrayOID:        arrayDecs(dateTextDec, dateBinDec, typ.Time),
	pgtype.TimestampArrayOID:   arrayDecs(tsTextDec, tsBinDec, typ.Time),
	pgtype.TimestamptzArrayOID: arrayDecs(tstzTextDec, tstzBinDec, typ.Time),
	1183 /*TimeArrayOID*/ :     arrayDecs(timeTextDec, timeBinDec, typ.Span),
	1187 /*IntervalArrayOID*/ : arrayDecs(intervalTextDec, intervalBinDec, typ.Span),
	199 /* JSONArrayOID*/ :     arrayDecs(jsonDec, jsonDec, typ.Data),
	pgtype.JSONBArrayOID:       arrayDecs(jsonDec, jsonbDec, typ.Data),
}

func errDecoder(raw []byte, _ *lit.Reg) (lit.Val, error) {
	return nil, fmt.Errorf("decoder not implemented")
}

// following is a simplified rewrite of the pgtype decoders for oids used by daql.
// we handle null checks outside of the decoder, and use a local timezone for date and timestamp.
// we also reimplemented the text array parser to use less allocations.

func boolTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if len(raw) != 1 {
		return nil, fmt.Errorf("invalid length for bool: %d", len(raw))
	}
	return lit.Bool(raw[0] == 't'), nil
}
func boolBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if len(raw) != 1 {
		return nil, fmt.Errorf("invalid length for bool: %d", len(raw))
	}
	return lit.Bool(raw[0] == 1), nil
}

func rawBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	return lit.Raw(raw), nil
}
func rawTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if !bytes.HasPrefix(raw, []byte(`\x`)) {
		return nil, fmt.Errorf("invalid hex format")
	}
	n, err := hex.Decode(raw[2:], raw)
	return lit.Raw(raw[:n]), err
}

func intTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	return lit.Int(n), err
}
func int2BinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 2; len(raw) != n {
		return nil, fmt.Errorf("invalid length for int%d: %d", n, len(raw))
	}
	return lit.Int(int16(binary.BigEndian.Uint16(raw))), nil
}
func int4BinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 4; len(raw) != n {
		return nil, fmt.Errorf("invalid length for int%d: %d", n, len(raw))
	}
	return lit.Int(int32(binary.BigEndian.Uint32(raw))), nil
}
func int8BinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 8; len(raw) != n {
		return nil, fmt.Errorf("invalid length for int%d: %d", n, len(raw))
	}
	return lit.Int(binary.BigEndian.Uint64(raw)), nil
}

func realTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	n, err := strconv.ParseFloat(string(raw), 64)
	return lit.Real(n), err
}
func real4BinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 4; len(raw) != n {
		return nil, fmt.Errorf("invalid length for float%d: %d", n, len(raw))
	}
	d := binary.BigEndian.Uint32(raw)
	return lit.Real(math.Float32frombits(d)), nil
}
func real8BinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 8; len(raw) != n {
		return nil, fmt.Errorf("invalid length for float%d: %d", n, len(raw))
	}
	d := binary.BigEndian.Uint64(raw)
	return lit.Real(math.Float64frombits(d)), nil
}

func strDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	return lit.Str(raw), nil
}

func uuidTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	u, err := cor.ParseUUID(string(raw))
	return lit.UUID(u), err
}
func uuidBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 16; len(raw) != n {
		return nil, fmt.Errorf("invalid length for uuid: %d", len(raw))
	}
	var u lit.UUID
	copy(u[:], raw)
	return u, nil
}

func dateTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	s := string(raw)
	switch s {
	case "infinity", "-infinity":
		return lit.Time{}, nil
	}
	t, err := time.ParseInLocation("2006-01-02", s, time.Local)
	return lit.Time(t), err
}
func dateBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 4; len(raw) != n {
		return nil, fmt.Errorf("invalid length for date: %d", len(raw))
	}
	day := int32(binary.BigEndian.Uint32(raw))
	switch day {
	case math.MaxInt32, math.MinInt32:
		return lit.Time{}, nil
	}
	t := time.Date(2000, 1, 1+int(day), 0, 0, 0, 0, time.Local)
	return lit.Time(t), nil
}

func tsTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	s := string(raw)
	switch s {
	case "infinity", "-infinity":
		return lit.Time{}, nil
	}
	t, err := time.Parse("2006-01-02 15:04:05.999999999", s)
	return lit.Time(t.In(time.Local)), err
}
func tsBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 8; len(raw) != n {
		return nil, fmt.Errorf("invalid length for timestamp: %d", len(raw))
	}
	µs := int64(binary.BigEndian.Uint64(raw))
	switch µs {
	case math.MaxInt64, math.MinInt64:
		return lit.Time{}, nil
	}
	const sUnixToY2k = 946684800
	t := time.Unix(
		(µs/1_000_000)+sUnixToY2k,
		(µs%1_000_000)*1_000,
	)
	return lit.Time(t), nil
}

func tstzTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	s := string(raw)
	switch s {
	case "infinity", "-infinity":
		return lit.Time{}, nil
	}
	format := "2006-01-02 15:04:05.999999999Z07"
	if len(s) < 14 {
		return nil, fmt.Errorf("invalid length for timestamptz: %d", len(s))
	}
	if r := s[len(s)-9]; r == '-' || r == '+' {
		format = "2006-01-02 15:04:05.999999999Z07:00:00"
	} else if r = s[len(s)-6]; r == '-' || r == '+' {
		format = "2006-01-02 15:04:05.999999999Z07:00"
	}
	t, err := time.Parse(format, s)
	return lit.Time(t.In(time.Local)), err
}

const sUnixToY2k = 946684800

func tstzBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 8; len(raw) != n {
		return nil, fmt.Errorf("invalid length for timestamptz: %d", len(raw))
	}
	µs := int64(binary.BigEndian.Uint64(raw))
	switch µs {
	case math.MaxInt64, math.MinInt64:
		return lit.Time{}, nil
	}
	t := time.Unix(
		(µs/1_000_000)+sUnixToY2k,
		(µs%1_000_000)*1_000,
	)
	return lit.Time(t), nil
}

func timeTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if len(raw) < 8 {
		return nil, fmt.Errorf("invalid length for time %d", len(raw))
	}
	var res time.Duration
	µps := strings.SplitN(string(raw), ".", 2)
	tps := strings.SplitN(µps[0], ":", 3)
	if len(tps) != 3 {
		return nil, fmt.Errorf("invalid time %s", raw)
	}
	for i, u := range []time.Duration{time.Hour, time.Minute, time.Second} {
		n, err := strconv.ParseInt(tps[i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid time value %s", raw)
		}
		res += time.Duration(n) * u
	}
	if len(µps) < 2 {
		return lit.Span(res), nil
	}
	µp := µps[1]
	n, err := strconv.ParseInt(µp, 10, 64)
	if len(µp) > 6 || err != nil {
		return nil, fmt.Errorf("invalid time µs value %s", µp)
	}
	for c := len(µp); c < 6; c++ {
		n *= 10
	}
	return lit.Span(res + time.Duration(n)*time.Microsecond), nil
}
func timeBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 8; len(raw) != n {
		return nil, fmt.Errorf("invalid length for time: %d", len(raw))
	}
	res := time.Duration(int64(binary.BigEndian.Uint64(raw))) * time.Microsecond
	return lit.Span(res), nil
}

func intervalTextDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	dps := strings.Split(string(raw), " ")
	var res time.Duration
	for i := 0; i < len(dps)-1; i += 2 {
		n, err := strconv.ParseInt(dps[i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid interval date value %s", dps[i])
		}
		// i don't like the year and month normalization. but there is no sane way to do it.
		// this it what postgres uses if you extract(epoch from '1 year 1 month'::interval).
		switch u := dps[i+1]; u {
		case "year", "years":
			res += time.Duration(n) * 8766 * time.Hour // 365.25*24
		case "mon", "mons":
			res += time.Duration(n) * 720 * time.Hour
		case "day", "days":
			res += time.Duration(n) * 24 * time.Hour
		default:
			return nil, fmt.Errorf("unexpected interval date unit %s", u)
		}
	}
	if len(dps)%2 == 0 {
		return lit.Span(res), nil
	}
	var tres time.Duration
	t := dps[len(dps)-1]
	neg := len(t) > 0 && t[0] == '-'
	if neg {
		t = t[1:]
	}
	µps := strings.SplitN(t, ".", 2)
	tps := strings.SplitN(µps[0], ":", 3)
	if len(tps) != 3 {
		return nil, fmt.Errorf("invalid interval time %s", t)
	}
	for i, u := range []time.Duration{time.Hour, time.Minute, time.Second} {
		n, err := strconv.ParseInt(tps[i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid interval time value %s", tps[i])
		}
		tres += time.Duration(n) * u
	}
	if len(µps) == 2 {
		µp := µps[1]
		n, err := strconv.ParseInt(µp, 10, 64)
		if len(µp) > 6 || err != nil {
			return nil, fmt.Errorf("invalid interval µs value %s", µp)
		}
		for c := len(µp); c < 6; c++ {
			n *= 10
		}
		tres += time.Duration(n) * time.Microsecond
	}
	if neg {
		res -= tres
	} else {
		res += tres
	}
	return lit.Span(res), nil
}
func intervalBinDec(raw []byte, _ *lit.Reg) (lit.Val, error) {
	if n := 16; len(raw) != n {
		return nil, fmt.Errorf("invalid length for interval: %d", len(raw))
	}
	µ := time.Duration(int64(binary.BigEndian.Uint64(raw[:8]))) * time.Microsecond
	d := time.Duration(int32(binary.BigEndian.Uint32(raw[8:12]))) * 24 * time.Hour
	// XXX: this is not great, but it is how it is in postgres.
	m := time.Duration(int32(binary.BigEndian.Uint32(raw[12:]))) * 30 * 24 * time.Hour
	return lit.Span(µ + d + m), nil
}

func jsonDec(raw []byte, reg *lit.Reg) (lit.Val, error) {
	return lit.Read(reg, bytes.NewReader(raw), "json")
}
func jsonbDec(raw []byte, reg *lit.Reg) (lit.Val, error) {
	if len(raw) == 0 || raw[0] != 1 {
		return nil, fmt.Errorf("invalid jsonb version")
	}
	return lit.Read(reg, bytes.NewReader(raw[1:]), "jsonb")
}

func arrayDecs(txt, bin Decoder, t typ.Type) DecoderPair {
	return DecoderPair{arrayTextDec(txt, t), arrayBinDec(bin, t)}
}
func arrayTextDec(eldec Decoder, elt typ.Type) Decoder {
	return func(raw []byte, reg *lit.Reg) (lit.Val, error) {
		a, err := parseRawTextArray(raw)
		if err != nil {
			return nil, err
		}
		// allocate one long slice and dice it for uncommen nested arrays
		vals := make([]lit.Val, len(a.Els))
		for i, el := range a.Els {
			if el.IsNull() {
				if elt.Kind&knd.None == 0 {
					elt = typ.Opt(elt)
				}
				vals[i] = lit.Null{}
			} else {
				val, err := eldec(el.Raw, reg)
				if err != nil {
					return nil, err
				}
				vals[i] = val
			}
		}
		return makeList(elt, vals, a.Dims), nil
	}
}
func arrayBinDec(eldec Decoder, elt typ.Type) Decoder {
	return func(raw []byte, reg *lit.Reg) (lit.Val, error) {
		var hdr pgtype.ArrayHeader
		off, err := hdr.DecodeBinary(nil, raw)
		if err != nil {
			return nil, err
		}
		if len(hdr.Dimensions) == 0 {
			return lit.Null{}, nil
		}
		n := hdr.Dimensions[0].Length
		for _, d := range hdr.Dimensions[1:] {
			n *= d.Length
		}
		// allocate one long slice and dice it for uncommen nested arrays
		vals := make([]lit.Val, n)
		for i := range vals {
			size := int(int32(binary.BigEndian.Uint32(raw[off:])))
			off += 4
			if size < 0 {
				if elt.Kind&knd.None == 0 {
					elt = typ.Opt(elt)
				}
				vals[i] = lit.Null{}
			} else {
				elraw := raw[off : off+size]
				off += size
				val, err := eldec(elraw, reg)
				if err != nil {
					return nil, err
				}
				vals[i] = val
			}
		}
		return makeList(elt, vals, hdr.Dimensions), nil
	}
}

func makeList(elt typ.Type, vals []lit.Val, dims []pgtype.ArrayDimension) *lit.List {
	if len(dims) > 1 {
		for d := len(dims) - 1; d >= 0; d-- {
			size := int(dims[d].Length)
			cur := make([]lit.Val, 0, len(vals)/size)
			for i := 0; i < len(vals); i += size {
				cur = append(cur, &lit.List{El: elt, Vals: vals[i : i+size]})
			}
			vals = cur
			elt = typ.ListOf(elt)
		}
	}
	return &lit.List{El: elt, Vals: vals}
}
