// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package memsimplekv_test

import (
	"testing"

	"github.com/juju/simplekv"
	"github.com/juju/simplekv/internal/simplekvtest"
	"github.com/juju/simplekv/memsimplekv"
)

func TestMemStore(t *testing.T) {
	simplekvtest.TestStore(t, func() (simplekv.Store, error) {
		return memsimplekv.NewStore(), nil
	})
}
