package qrypgx

import (
	"fmt"

	"xelf.org/daql/qry"
	"xelf.org/xelf/exp"
	"xelf.org/xelf/mod"
)

// Mod is the xelf module source for this package that ensures provider registration.
var Mod *mod.Src

func init() {
	Mod = mod.Registry.Register(&mod.Src{
		Rel:   "qrypgx",
		Loc:   mod.Loc{URL: "xelf:qrypgx"},
		Setup: modSetup,
	})
}

func modSetup(prog *exp.Prog, s *mod.Src) (f *mod.File, err error) {
	// ensure the dom module is loaded
	if f := prog.Files[qry.Mod.URL]; f == nil {
		le := mod.FindLoaderEnv(prog.Root)
		if le == nil {
			return nil, fmt.Errorf("no loader env found")
		}
		f, err = le.LoadFile(prog, &qry.Mod.Loc)
		if err != nil {
			return nil, err
		}
		err := prog.File.AddRefs(f.Refs...)
		if err != nil {
			return nil, err
		}
	}
	f = &exp.File{URL: s.URL}
	me := mod.NewModEnv(prog, f)
	me.SetName("qrypgx")
	return f, me.Publish()
}
