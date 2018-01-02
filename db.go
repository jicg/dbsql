package dbsql

import (
	"reflect"
)

const (
	defaultStructTagName  = "dbsql"
	defaultStructTagDelim = ";"
)

type dbBase struct {
}

type db_table struct {
	name     string
	cols     []*db_column
	extrasql []string
}

type db_column struct {
	name       string
	dbtype     string
	hasdef     bool
	defval     string
	primaryKey bool
	index      bool
	unique     bool
	notnull    bool
}

type dber interface {
	getCreateSql(*db_table) (string, error)

	getType(typ reflect.Type) string

	getAddColumnSql(table *db_table, col *db_column) string

	DBCheckTableSql(table string) string

	DBGetColumnsSql(table string) string

	DBCheckIndexSql(table string, name string) string
}
