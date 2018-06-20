// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package mgosimplekv_test

import (
	"github.com/juju/testing"
	gc "gopkg.in/check.v1"

	"github.com/juju/simplekv"
	"github.com/juju/simplekv/internal/simplekvtest"
	"github.com/juju/simplekv/mgosimplekv"
)

type kvSuite struct {
	testing.IsolatedMgoSuite
	simplekvtest.KeyValueSuite
}

var _ = gc.Suite(&kvSuite{})

func (s *kvSuite) SetUpSuite(c *gc.C) {
	s.IsolatedMgoSuite.SetUpSuite(c)
	s.KeyValueSuite.SetUpSuite(c)
}

func (s *kvSuite) TearDownSuite(c *gc.C) {
	s.KeyValueSuite.TearDownSuite(c)
	s.IsolatedMgoSuite.TearDownSuite(c)
}

func (s *kvSuite) SetUpTest(c *gc.C) {
	s.IsolatedMgoSuite.SetUpTest(c)
	s.NewStore = func() (simplekv.Store, error) {
		return mgosimplekv.NewStore(s.Session.DB("test").C("test"))
	}
	s.KeyValueSuite.SetUpTest(c)
}

func (s *kvSuite) TearDownTest(c *gc.C) {
	s.KeyValueSuite.TearDownTest(c)
	s.IsolatedMgoSuite.TearDownTest(c)
}
