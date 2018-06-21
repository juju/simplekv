// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package sqlsimplekv

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/lib/pq"
	errgo "gopkg.in/errgo.v1"
)

const postgresInitTmpl = `
CREATE TABLE IF NOT EXISTS {{.TableName}} ( 
	key TEXT NOT NULL,
	value BYTEA NOT NULL,
	expire TIMESTAMP WITH TIME ZONE,
	UNIQUE (key)
);

CREATE OR REPLACE FUNCTION {{.TableName}}_expire_fn() RETURNS trigger
LANGUAGE plpgsql
AS $$
	BEGIN
		DELETE FROM {{.TableName}} WHERE expire < NOW();
		RETURN NEW;
	END;
$$;

CREATE INDEX IF NOT EXISTS {{.TableName}}_expire ON {{.TableName}} (expire);
DROP TRIGGER IF EXISTS {{.TableName}}_expire_tr ON {{.TableName}};
CREATE TRIGGER {{.TableName}}_expire_tr
   BEFORE INSERT ON {{.TableName}}
   EXECUTE PROCEDURE {{.TableName}}_expire_fn();
`

var postgresTmpls = [numTmpl]string{
	tmplGetKeyValue: `
		SELECT value FROM {{.TableName}}
		WHERE key={{.Key | .Arg}} AND (expire IS NULL OR expire > now())`,
	tmplGetKeyValueForUpdate: `
		SELECT value FROM {{.TableName}}
		WHERE key={{.Key | .Arg}} AND (expire IS NULL OR expire > now())
		FOR UPDATE`,
	tmplInsertKeyValue: `
		INSERT INTO {{.TableName}} (key, value, expire)
		VALUES ({{.Key | .Arg}}, {{.Value | .Arg}}, {{.Expire | .Arg}})
		{{if .Update}}ON CONFLICT (key) DO UPDATE
		SET value={{.Value | .Arg}}, expire={{.Expire | .Arg}}{{end}}`,
}

// newPostgresDriver creates a postgres driver using the given DB.
func newPostgresDriver(db *sql.DB, tableName string) (*driver, error) {
	tmpl, err := template.New("").Parse(postgresInitTmpl)
	if err != nil {
		return nil, errgo.Mask(err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, keyValueParams{
		TableName: tableName,
	}); err != nil {
		return nil, errgo.Mask(err)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		return nil, errgo.Mask(err)
	}
	d := &driver{
		argBuilderFunc: func() argBuilder {
			return &postgresArgBuilder{}
		},
		isDuplicate: postgresIsDuplicate,
	}
	for i, t := range postgresTmpls {
		if err := d.parseTemplate(tmplID(i), t); err != nil {
			return nil, errgo.Notef(err, "cannot parse template %v", t)
		}
	}
	return d, nil
}

func postgresIsDuplicate(err error) bool {
	if pqerr, ok := err.(*pq.Error); ok && pqerr.Code.Name() == "unique_violation" {
		return true
	}
	return false
}

// postgresArgBuilder implements an argBuilder that produces placeholders
// in the the "$n" format.
type postgresArgBuilder struct {
	args_ []interface{}
}

// Arg implements argbuilder.Arg.
func (b *postgresArgBuilder) Arg(a interface{}) string {
	b.args_ = append(b.args_, a)
	return fmt.Sprintf("$%d", len(b.args_))
}

// args implements argbuilder.args.
func (b *postgresArgBuilder) args() []interface{} {
	return b.args_
}
