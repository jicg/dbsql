package dbsql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var oracleTypes = map[string]string{
	"string":          "VARCHAR2(%d)",
	"string-text":     "VARCHAR2(%d)",
	"time.Time-date":  "DATE",
	"time.Time":       "TIMESTAMP",
	"int":             "NUMBER(%d)",
	"int8":            "NUMBER(%d)",
	"int16":           "NUMBER(%d)",
	"int32":           "NUMBER(%d)",
	"int64":           "NUMBER(%d)",
	"uint":            "NUMBER(%d)",
	"uint8":           "NUMBER(%d)",
	"uint16":          "NUMBER(%d)",
	"uint32":          "NUMBER(%d)",
	"uint64":          "NUMBER(%d)",
	"float32":         "NUMBER(%d,%d)",
	"float64":         "NUMBER(%d,%d)",
	"float64-decimal": "NUMBER(%d, %d)",
}

type dbOracle struct {
	dbBase
}

func (db *dbOracle) getCreateSql(table *db_table) (string, error) {
	if table == nil || table.cols == nil || len(table.cols) == 0 {
		return "", errors.New("table or columns is null")
	}
	sql := fmt.Sprintf("CREATE TABLE %s (\n", strings.ToUpper(table.name))
	collen := len(table.cols)
	for index, col := range table.cols {
		sql += fmt.Sprintf("  %s %s", strings.ToUpper(col.name), strings.ToUpper(col.dbtype))
		if !col.primaryKey {
			if col.hasdef {
				sql += fmt.Sprintf(" default %v", col.defval)
			}
			if col.notnull {
				sql += " not null "

			}
		} else {
			sql += " not null primary key"
		}

		if index+1 < collen {
			sql += ",\n"
		}
	}
	sql +=  "\n)"
	return sql, nil
}

func (db *dbOracle) getAddColumnSql(table *db_table, col *db_column) string {
	sql := fmt.Sprintf(" ALTER TABLE %s ADD  %s %s", strings.ToUpper(table.name), strings.ToUpper(col.name), strings.ToUpper(col.dbtype))
	if col.hasdef {
		sql += fmt.Sprintf(" default %v", col.defval)
	}
	if col.notnull {
		sql += " not null "

	}
	return sql
}

func (db *dbOracle) getType(typ reflect.Type) string {
	return oracleTypes[typ.Name()]
}

func (d *dbOracle) DBCheckTableSql(table string) string {
	return fmt.Sprintf("SELECT count(1) FROM USER_TABLES WHERE TABLE_NAME = '%s'", strings.ToUpper(table))
}

// Oracle
func (d *dbOracle) DBGetColumnsSql(table string) string {
	return fmt.Sprintf("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS "+
		"WHERE TABLE_NAME ='%s'", strings.ToUpper(table))
}

// check index is exist
func (d *dbOracle) DBCheckIndexSql(table string, name string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM USER_IND_COLUMNS, USER_INDEXES "+
		"WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME "+
		"AND  USER_IND_COLUMNS.TABLE_NAME = '%s' AND USER_IND_COLUMNS.INDEX_NAME = '%s'",
		strings.ToUpper(table), strings.ToUpper(name))
}
