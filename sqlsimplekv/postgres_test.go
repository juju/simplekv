// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package sqlsimplekv_test

import (
	"github.com/juju/postgrestest"
	gc "gopkg.in/check.v1"
	errgo "gopkg.in/errgo.v1"

	"github.com/juju/simplekv"
	"github.com/juju/simplekv/internal/simplekvtest"
	"github.com/juju/simplekv/sqlsimplekv"
)

type postgresKeyValueSuite struct {
	simplekvtest.KeyValueSuite
	pg *postgrestest.DB
}

var _ = gc.Suite(&postgresKeyValueSuite{})

func (s *postgresKeyValueSuite) SetUpTest(c *gc.C) {
	pg, err := postgrestest.New()
	if errgo.Cause(err) == postgrestest.ErrDisabled {
		c.Skip(err.Error())
		return
	}
	c.Assert(err, gc.Equals, nil)
	s.pg = pg
	s.NewStore = func() (simplekv.Store, error) {
		return sqlsimplekv.NewStore("postgres", s.pg.DB, "test")
	}
	s.KeyValueSuite.SetUpTest(c)
}

func (s *postgresKeyValueSuite) TearDownTest(c *gc.C) {
	if s.pg != nil {
		s.pg.Close()
	}
	s.KeyValueSuite.TearDownTest(c)
}
