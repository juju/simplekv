// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package sqlsimplekv_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/juju/postgrestest"
	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
	"github.com/juju/simplekv/internal/simplekvtest"
	"github.com/juju/simplekv/sqlsimplekv"
)

func TestPostgresStore(t *testing.T) {
	pg, err := postgrestest.New()
	if err != nil {
		if errgo.Cause(err) == postgrestest.ErrDisabled {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer pg.Close()
	var id int32
	simplekvtest.TestStore(t, func() (_ simplekv.Store, err error) {
		table := fmt.Sprintf("test%d", atomic.AddInt32(&id, 1))
		return sqlsimplekv.NewStore("postgres", pg.DB, table)
	})
}
