// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pgsimplekv

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"time"

	"github.com/lib/pq"
	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
)

// NewStore returns a new Store instance that uses the
// given sql database for storage.
// The data will be stored in a table with the given name
// (other SQL artificacts may also be created using the name as a prefix).
func NewStore(db *sql.DB, tableName string) (*Store, error) {
	kvs := &Store{
		tableName: tableName,
		db:        db,
	}
	if err := kvs.init(); err != nil {
		return nil, errgo.Mask(err)
	}
	return kvs, nil
}

// A Store implements simplekv.Store.
type Store struct {
	db        *sql.DB
	tableName string
	stmts     [numQueries]*sql.Stmt
}

func (s *Store) init() error {
	params := struct {
		TableName string
	}{
		TableName: s.tableName,
	}

	var b bytes.Buffer
	if err := tmplInit.Execute(&b, params); err != nil {
		return errgo.Mask(err)
	}
	if _, err := s.db.Exec(b.String()); err != nil {
		return errgo.Mask(err)
	}
	for i := range s.stmts {
		b.Reset()
		if err := tmpls[i].Execute(&b, params); err != nil {
			return errgo.Mask(err)
		}
		stmt, err := s.db.Prepare(b.String())
		if err != nil {
			return errgo.Mask(err)
		}
		s.stmts[i] = stmt
	}
	return nil
}

func (s *Store) Close() error {
	var ferr error
	for i, stmt := range s.stmts {
		if stmt == nil {
			continue
		}
		if err := stmt.Close(); err != nil {
			// just return the first error
			if ferr == nil {
				ferr = err
			}
		}
		s.stmts[i] = nil
	}
	return nil
}

// Context implements simplekv.Store.Context.
func (s *Store) Context(ctx context.Context) (context.Context, func()) {
	return ctx, func() {}
}

// Get implements simplekv.Store.Get by selecting the blob with the
// given key from the table.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	var v []byte
	if err := s.stmts[querySelect].QueryRowContext(ctx, key).Scan(&v); err != nil {
		if errgo.Cause(err) == sql.ErrNoRows {
			return nil, simplekv.KeyNotFoundError(key)
		}
		return nil, errgo.Mask(err)
	}
	return v, nil
}

// Set implements simplekv.Store.Set by upserting the blob with the
// given key, value and expire time into the table.
func (s *Store) Set(ctx context.Context, key string, value []byte, expire time.Time) error {
	_, err := s.stmts[queryUpsert].ExecContext(ctx, key, value, nullTime{Time: expire, Valid: !expire.IsZero()})
	return errgo.Mask(err)
}

// Update implements simplekv.Store.Update.
func (s *Store) Update(ctx context.Context, key string, expire time.Time, getVal func(old []byte) ([]byte, error)) error {
	for {
		insertOnly := false
		err := s.withTx(func(tx *sql.Tx) error {
			var v []byte
			stmt := tx.Stmt(s.stmts[querySelectForUpdate])
			defer stmt.Close()
			if err := stmt.QueryRowContext(ctx, key).Scan(&v); err != nil {
				if errgo.Cause(err) != sql.ErrNoRows {
					return errgo.Mask(err)
				}
				// The document doesn't exist, so we want to fail if some other process
				// has inserted it concurrently.
				insertOnly = true
			} else if v == nil {
				v = []byte{}
			}
			newVal, err := getVal(v)
			if err != nil {
				return errgo.Mask(err, errgo.Any)
			}
			if insertOnly {
				stmt = tx.Stmt(s.stmts[queryInsert])
				defer stmt.Close()
			} else {
				stmt = tx.Stmt(s.stmts[queryUpdate])
				defer stmt.Close()
			}
			if _, err := stmt.ExecContext(ctx, key, newVal, nullTime{Time: expire, Valid: !expire.IsZero()}); err != nil {
				return errgo.Mask(err, isDuplicate)
			}
			return nil
		})
		if !insertOnly || !isDuplicate(errgo.Cause(err)) {
			return errgo.Mask(err, errgo.Any)
		}
		// The document didn't previously exist (so we couldn't lock it) but when we
		// tried the insert, it failed with a duplicate-key error and aborted the transaction,
		// so we'll now try again with the document in place.
	}
}

type nullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements sql.Scanner.
func (n *nullTime) Scan(src interface{}) error {
	if src == nil {
		n.Time = time.Time{}
		n.Valid = false
		return nil
	}
	if t, ok := src.(time.Time); ok {
		n.Time = t
		n.Valid = true
		return nil
	}
	return errgo.Newf("unsupported Scan, storing driver.Value type %T into type %T", src, n)
}

// Value implements sqldriver.Valuer.
func (n nullTime) Value() (sqldriver.Value, error) {
	if n.Valid {
		return n.Time, nil
	}
	return nil, nil
}

// withTx runs f in a new transaction. any error returned by f will not
// have it's cause masked.
func (s *Store) withTx(f func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return errgo.Mask(err)
	}
	if err := f(tx); err != nil {
		if err1 := tx.Rollback(); err1 != nil {
			return errgo.NoteMask(err, fmt.Sprintf("failed to roll back (error: %v) after error", err1), errgo.Any)
		}
		return errgo.Mask(err, errgo.Any)
	}
	return errgo.Mask(tx.Commit())
}

func isDuplicate(err error) bool {
	if pqerr, ok := err.(*pq.Error); ok && pqerr.Code.Name() == "unique_violation" {
		return true
	}
	return false
}
