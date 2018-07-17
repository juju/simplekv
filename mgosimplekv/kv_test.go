// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package mgosimplekv_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/juju/mgotest"
	"github.com/juju/simplekv"
	"github.com/juju/simplekv/internal/simplekvtest"
	"github.com/juju/simplekv/mgosimplekv"
	errgo "gopkg.in/errgo.v1"
)

func TestMgoStore(t *testing.T) {
	db, err := mgotest.New()
	if err != nil {
		if errgo.Cause(err) == mgotest.ErrDisabled {
			t.Skip(err)
		}
		t.Fatal(err)
	}

	defer db.Close()
	var id int32
	simplekvtest.TestStore(t, func() (_ simplekv.Store, err error) {
		coll := fmt.Sprintf("test%d", atomic.AddInt32(&id, 1))
		store, err := mgosimplekv.NewStore(db.C(coll))
		if err != nil {
			return nil, errgo.Mask(err)
		}
		return store, nil
	})
}
