package main

import (
	"bufio"
	"os"

	"xelf.org/dapgx"
	"xelf.org/dapgx/dompgx"
	_ "xelf.org/dapgx/evtpgx"
	_ "xelf.org/dapgx/qrypgx"
	"xelf.org/daql"
	"xelf.org/daql/gen"
	"xelf.org/xelf/xps"
)

func Cmd(ctx *xps.CmdCtx) error {
	switch ctx.Split() {
	case "", "gen":
	default:
		return nil
	}
	pr, ss, err := daql.LoadProjectSchemas(ctx.Dir, ctx.Args)
	if err != nil {
		return err
	}
	b := bufio.NewWriter(os.Stdout)
	defer b.Flush()
	w := dapgx.NewWriter(b, pr.Project, nil, nil)
	w.WriteString(w.Header)
	w.WriteString("BEGIN;\n\n")
	for _, s := range ss {
		if gen.Nogen(s) {
			continue
		}
		err := dompgx.WriteSchema(w, s)
		if err != nil {
			return err
		}
	}
	w.WriteString("COMMIT;\n")
	return b.Flush()
}
