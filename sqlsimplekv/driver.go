// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package sqlsimplekv

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"text/template"

	errgo "gopkg.in/errgo.v1"
)

type tmplID int

const (
	_ tmplID = iota - 1
	tmplGetKeyValue
	tmplGetKeyValueForUpdate
	tmplInsertKeyValue
	tmplListKeys
	numTmpl
)

type queryer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// argBuilder is an interface that can be embedded in template parameters
// to record the arguments needed to be supplied with SQL queries.
type argBuilder interface {
	// Arg is a method that is called in templates with the value of
	// the next argument to be used in the query. Arg should remmebre
	// the value and return a valid placeholder to access that
	// argument when executing the query.
	Arg(interface{}) string

	// args returns the slice of arguments that should be used when
	// executing the query.
	args() []interface{}
}

type driver struct {
	tmpls          [numTmpl]*template.Template
	argBuilderFunc func() argBuilder
	isDuplicate    func(error) bool
}

// exec performs the Exec method on the given queryer by processing the
// given template with the given params to determine the query to
// execute.
func (d *driver) exec(ctx context.Context, q queryer, tmplID tmplID, params argBuilder) (sql.Result, error) {
	query, err := d.executeTemplate(tmplID, params)
	if err != nil {
		return nil, errgo.Notef(err, "cannot build query")
	}
	res, err := q.ExecContext(ctx, query, params.args()...)
	return res, errgo.Mask(err, errgo.Any)
}

// query performs the Query method on the given queryer by processing the
// given template with the given params to determine the query to
// execute.
func (d *driver) query(ctx context.Context, q queryer, tmplID tmplID, params argBuilder) (*sql.Rows, error) {
	query, err := d.executeTemplate(tmplID, params)
	if err != nil {
		return nil, errgo.Notef(err, "cannot build query")
	}
	rows, err := q.QueryContext(ctx, query, params.args()...)
	return rows, errgo.Mask(err, errgo.Any)
}

// queryRow performs the QueryRow method on the given queryer by
// processing the given template with the given params to determine the
// query to execute.
func (d *driver) queryRow(ctx context.Context, q queryer, tmplID tmplID, params argBuilder) (*sql.Row, error) {
	query, err := d.executeTemplate(tmplID, params)
	if err != nil {
		return nil, errgo.Notef(err, "cannot build query")
	}
	return q.QueryRowContext(ctx, query, params.args()...), nil
}

func (d *driver) parseTemplate(tmplID tmplID, tmpl string) error {
	var err error
	d.tmpls[tmplID], err = template.New("").Funcs(template.FuncMap{
		"join": strings.Join,
	}).Parse(tmpl)
	return errgo.Mask(err)
}

func (d *driver) executeTemplate(tmplID tmplID, params argBuilder) (string, error) {
	var buf bytes.Buffer
	if err := d.tmpls[tmplID].Execute(&buf, params); err != nil {
		return "", errgo.Mask(err)
	}
	return buf.String(), nil
}
