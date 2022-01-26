package dapgx

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"

	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"xelf.org/xelf/lit"
)

type C interface {
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

type PC interface {
	C
	Prepare(context.Context, string, string) (*pgconn.StatementDescription, error)
}

var _ C = (*pgxpool.Pool)(nil)
var _ PC = (*pgx.Conn)(nil)
var _ PC = (pgx.Tx)(nil)

type DB interface {
	Begin(context.Context) (pgx.Tx, error)
}

func WithTx(ctx context.Context, db DB, f func(PC) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	err = f(tx)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func Open(ctx context.Context, dsn string, logger pgx.Logger) (*pgxpool.Pool, error) {
	db, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating pgx connection pool: %w", err)
	}
	_, err = db.Exec(ctx, "SELECT 1")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("opening first pgx connection: %w", err)
	}
	return db, nil
}

func Query(ctx context.Context, pc PC, sql string, args []lit.Val) (pgx.Rows, error) {
	name, wrap, err := prep(ctx, pc, sql, args)
	if err != nil {
		return nil, err
	}
	return pc.Query(ctx, name, wrap...)
}

func Exec(ctx context.Context, pc PC, sql string, args []lit.Val) error {
	name, wrap, err := prep(ctx, pc, sql, args)
	if err != nil {
		return err
	}
	_, err = pc.Exec(ctx, name, wrap...)
	return err
}

func prep(ctx context.Context, pc PC, sql string, args []lit.Val) (string, []interface{}, error) {
	name := hashSql(sql)
	sd, err := pc.Prepare(ctx, name, sql)
	if err != nil {
		return "", nil, err
	}
	if len(sd.ParamOIDs) != len(args) {
		return "", nil, fmt.Errorf("invalid number of params")
	}
	res := make([]interface{}, len(args))
	for i, oid := range sd.ParamOIDs {
		enc, err := FieldEncoder(oid, args[i])
		if err != nil {
			return "", nil, err
		}
		res[i] = enc
	}
	return name, res, nil
}

func hashSql(sql string) string {
	h := sha1.New()
	io.WriteString(h, sql)
	return fmt.Sprintf("%x", h.Sum(nil))
}
