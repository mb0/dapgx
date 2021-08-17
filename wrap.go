package dapgx

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgtype"
	"xelf.org/xelf/bfr"
	"xelf.org/xelf/lit"
)

func WrapArg(arg interface{}) interface{} {
	switch p := arg.(type) {
	case *lit.Strc:
		return &MutWrap{p}
	case *lit.Dict:
		return &MutWrap{p}
	case *lit.Map:
		return &MutWrap{p}
	case *lit.List:
		return &MutWrap{p}
	}
	if mut, ok := arg.(lit.Mut); ok {
		return mut.Ptr()
	}
	return arg
}

func WrapPtr(reg *lit.Reg, ptr interface{}) interface{} {
	switch p := ptr.(type) {
	case *lit.Time:
		return (*time.Time)(p)
	case **lit.Strc:
		v := *p
		if v == nil {
			v = &lit.Strc{Reg: reg}
			*p = v
		}
		return &MutWrap{v}
	case **lit.Dict:
		v := *p
		if v == nil {
			v = &lit.Dict{Reg: reg}
			*p = v
		}
		return &MutWrap{v}
	case **lit.Map:
		v := *p
		if v == nil {
			v = &lit.Map{Reg: reg}
			*p = v
		}
		return &MutWrap{v}
	case **lit.List:
		v := *p
		if v == nil {
			v = &lit.List{Reg: reg}
			*p = v
		}
		return &MutWrap{v}
	}
	return ptr
}

type MutWrap struct {
	Mut UnmarshalMut
}

type UnmarshalMut interface {
	lit.Mut
	json.Unmarshaler
}

func (m *MutWrap) EncodeText(c *pgtype.ConnInfo, b []byte) ([]byte, error) {
	if m.Mut == nil || m.Mut.Nil() {
		return nil, nil
	}
	res, err := bfr.JSON(m.Mut)
	if err != nil {
		return b, err
	}
	return append(b, res...), nil
}

func (m *MutWrap) DecodeText(c *pgtype.ConnInfo, b []byte) error {
	if m.Mut == nil {
		return fmt.Errorf("cannot decode into nil mut")
	}
	if len(b) == 0 {
		return m.Mut.Assign(lit.Null{})
	}
	return m.Mut.UnmarshalJSON(b)
}
