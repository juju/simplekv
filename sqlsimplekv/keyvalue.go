// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package sqlsimplekv

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"time"

	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
)

// NewStore returns a new Store instance that uses the
// given sql database for storage, generating SQL with the
// given driver (currently only "postgres" is supported).
//
// The data will be stored in a table with the given name
// (other SQL artificacts may also be created using the name as a prefix).
func NewStore(driverName string, db *sql.DB, tableName string) (simplekv.Store, error) {
	if driverName != "postgres" {
		return nil, errgo.Newf("unsupported database driver %q", driverName)
	}
	driver, err := newPostgresDriver(db, tableName)
	if err != nil {
		return nil, errgo.Notef(err, "cannot initialise database")
	}
	return &kvStore{
		tableName: tableName,
		db:        db,
		driver:    driver,
	}, nil
}

// A kvStore implements simplekv.Store.
type kvStore struct {
	db        *sql.DB
	driver    *driver
	tableName string
}

// Context implements simplekv.Store.Context.
func (s *kvStore) Context(ctx context.Context) (context.Context, func()) {
	return ctx, func() {}
}

type keyValueParams struct {
	argBuilder

	TableName string
	Key       string
	Value     []byte
	Expire    nullTime
	Update    bool
}

// Get implements simplekv.Store.Get by selecting the blob with the
// given key from the table.
func (s *kvStore) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := s.get(ctx, s.db, key, false)
	if err != nil {
		return nil, errgo.Mask(err, errgo.Is(simplekv.ErrNotFound))
	}
	return v, nil
}

// get is like Get except that it operates on a general queryer value.
// If forUpdate is true, it takes out a lock on the given key so that a subsequent
// call to set will happen atomically.
func (s *kvStore) get(ctx context.Context, q queryer, key string, forUpdate bool) ([]byte, error) {
	params := &keyValueParams{
		argBuilder: s.driver.argBuilderFunc(),
		TableName:  s.tableName,
		Key:        key,
	}
	var value []byte
	tmpl := tmplGetKeyValue
	if forUpdate {
		tmpl = tmplGetKeyValueForUpdate
	}
	row, err := s.driver.queryRow(ctx, q, tmpl, params)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	if err := row.Scan(&value); err != nil {
		if errgo.Cause(err) == sql.ErrNoRows {
			return nil, simplekv.KeyNotFoundError(key)
		}
		return nil, errgo.Mask(err)
	}
	return value, nil
}

// Set implements simplekv.Store.Set by upserting the blob with the
// given key, value and expire time into the table.
func (s *kvStore) Set(ctx context.Context, key string, value []byte, expire time.Time) error {
	return s.set(ctx, s.db, key, value, expire, false)
}

// set is like Set except that it operates on a general queryer value.
// If insertOnly is true, the value will only be set if the key doesn't exist.
func (s *kvStore) set(ctx context.Context, q queryer, key string, value []byte, expire time.Time, insertOnly bool) error {
	_, err := s.driver.exec(ctx, q, tmplInsertKeyValue, &keyValueParams{
		argBuilder: s.driver.argBuilderFunc(),
		TableName:  s.tableName,
		Key:        key,
		Value:      value,
		Expire:     nullTime{expire, !expire.IsZero()},
		Update:     !insertOnly,
	})
	if err != nil {
		return errgo.Mask(err, s.driver.isDuplicate)
	}
	return nil
}

// Update implements simplekv.Store.Update.
func (s *kvStore) Update(ctx context.Context, key string, expire time.Time, getVal func(old []byte) ([]byte, error)) error {
	for {
		insertOnly := false
		err := s.withTx(func(tx *sql.Tx) error {
			v, err := s.get(ctx, tx, key, true)
			if err != nil {
				if errgo.Cause(err) != simplekv.ErrNotFound {
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
			err = s.set(ctx, tx, key, newVal, expire, insertOnly)
			if err == nil {
				return nil
			}
			return errgo.Mask(err, s.driver.isDuplicate)
		})
		if !insertOnly || !s.driver.isDuplicate(errgo.Cause(err)) {
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
func (s *kvStore) withTx(f func(*sql.Tx) error) error {
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
