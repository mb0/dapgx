package main

import (
	"bufio"
	"os"

	"xelf.org/dapgx"
	"xelf.org/dapgx/dompgx"
	_ "xelf.org/dapgx/evtpgx"
	_ "xelf.org/dapgx/qrypgx"
	"xelf.org/daql/gen"
	"xelf.org/daql/xps/cmd"
)

func Cmd(dir string, args []string) error {
	_, args = split(args)
	fst, args := split(args)
	switch fst {
	case "gen":
		return genPg(dir, args)
	}
	return nil

}
func genPg(dir string, args []string) error {
	pr, ss, err := cmd.LoadProjectSchemas(dir, args)
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

func split(args []string) (string, []string) {
	if len(args) > 0 {
		return args[0], args[1:]
	}
	return "", nil
}
