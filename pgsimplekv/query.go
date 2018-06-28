// Copyright 2018 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pgsimplekv

import "text/template"

const (
	querySelect = iota
	querySelectForUpdate
	queryUpsert
	queryInsert
	queryUpdate
	numQueries
)

var tmplInit = template.Must(template.New("").Parse(`
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
`))

var tmpls = [numQueries]*template.Template{
	template.Must(template.New("").Parse(`
		SELECT value FROM {{.TableName}}
		WHERE key=$1 AND (expire IS NULL OR expire > now())`)),
	template.Must(template.New("").Parse(`
		SELECT value FROM {{.TableName}}
		WHERE key=$1 AND (expire IS NULL OR expire > now())
		FOR UPDATE`)),
	template.Must(template.New("").Parse(`
		INSERT INTO {{.TableName}} (key, value, expire)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE
		SET value=$2, expire=$3`)),
	template.Must(template.New("").Parse(`
		INSERT INTO {{.TableName}} (key, value, expire)
		VALUES ($1, $2, $3)`)),
	template.Must(template.New("").Parse(`
		UPDATE {{.TableName}}
		SET value=$2, expire=$3 WHERE key=$1`)),
}
