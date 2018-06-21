// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package memsimplekv_test

import (
	gc "gopkg.in/check.v1"

	"github.com/juju/simplekv"
	"github.com/juju/simplekv/internal/simplekvtest"
	"github.com/juju/simplekv/memsimplekv"
)

type keyvalueSuite struct {
	simplekvtest.KeyValueSuite
}

var _ = gc.Suite(&keyvalueSuite{})

func (s *keyvalueSuite) SetUpTest(c *gc.C) {
	s.NewStore = func() (simplekv.Store, error) {
		return memsimplekv.NewStore(), nil
	}
	s.KeyValueSuite.SetUpTest(c)
}
