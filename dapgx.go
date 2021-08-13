package dapgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var BG = context.Background()

type Ctx = context.Context

type C interface {
	Query(Ctx, string, ...interface{}) (pgx.Rows, error)
	QueryRow(Ctx, string, ...interface{}) pgx.Row
	Exec(Ctx, string, ...interface{}) (pgconn.CommandTag, error)
	Prepare(Ctx, string, string) (*pgconn.StatementDescription, error)
	CopyFrom(Ctx, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

type DB interface {
	Begin(Ctx) (pgx.Tx, error)
}

func WithTx(db DB, f func(C) error) error {
	tx, err := db.Begin(BG)
	if err != nil {
		return err
	}
	defer tx.Rollback(BG)
	err = f(tx)
	if err != nil {
		return err
	}
	return tx.Commit(BG)
}

func Open(dsn string, logger pgx.Logger) (*pgxpool.Pool, error) {
	db, err := pgxpool.Connect(BG, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating pgx connection pool: %w", err)
	}
	_, err = db.Exec(BG, "SELECT 1")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("opening first pgx connection: %w", err)
	}
	return db, nil
}
