package dbsql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"errors"

	"github.com/jmoiron/sqlx"
)

type DBsql struct {
	DBer dber
	db   *sqlx.DB
}

const (
	Oracle = "oracle"
)

var dbmap = map[string]dber{
	"oracle": &dbOracle{},
}

func New(db *sqlx.DB, otype string) *DBsql {
	return &DBsql{
		dbmap[otype],
		db,
	}
}

func (d *DBsql) Sync(model interface{}) error {
	if model == nil {
		return errors.New(" not exists models , invalid argument ! ")
	}
	table, err := d.Model2Table(model)
	if err != nil {
		return err
	}
	return d.SyscTable(table)
}

func (d *DBsql) SyncSqls(model interface{}) ([]string, []string, error) {
	if model == nil {
		return nil, nil, errors.New(" not exists models , invalid argument ! ")
	}
	table, err := d.Model2Table(model)
	if err != nil {
		return nil, nil, err
	}
	return d.SyscTableSqls(table)
}

func (d *DBsql) SyscTableSqls(table *db_table) ([]string, []string, error) {
	sqls := []string{}
	cnt := 0
	row := d.db.QueryRow(d.DBer.DBCheckTableSql(table.name))
	row.Scan(&cnt)
	if cnt == 0 {
		sql, err := d.DBer.getCreateSql(table)
		if err != nil {
			return nil, nil, err
		}
		sqls = append(sqls, sql)
		//if table.extrasql != nil && len(table.extrasql) > 0 {
		//	size := len(table.extrasql)
		//	for i := 0; i < size; i++ {
		//		sqls = append(sqls, table.extrasql[i])
		//	}
		//}
	} else {
		db_columns := []string{}
		sql := d.DBer.DBGetColumnsSql(table.name)
		rows, err := d.db.Query(sql)
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				return nil, nil, err
			}
			db_columns = append(db_columns, name)
		}
		err = rows.Err()
		if err != nil {
			return nil, nil, err
		}

		init_columns := []*db_column{}

		for _, col := range table.cols {
			flag := false
			for _, c2 := range db_columns {
				if strings.ToLower(col.name) == strings.ToLower(c2) {
					flag = true
					break
				}
			}
			if !flag {
				init_columns = append(init_columns, col)
			}
		}
		for _, col := range init_columns {
			sql := d.DBer.getAddColumnSql(table, col)
			sqls = append(sqls, sql)
		}
	}
	return sqls, table.extrasql, nil
}

func (d *DBsql) SyncSql2s(model interface{}) ([]string, []string, error) {
	if model == nil {
		return nil, nil, errors.New(" not exists models , invalid argument ! ")
	}
	table, err := d.Model2Table(model)
	if err != nil {
		return nil, nil, err
	}
	return d.SyscTableSql2s(table)
}
func (d *DBsql) SyscTableSql2s(table *db_table) ([]string, []string, error) {
	sqls := []string{}
	sql, err := d.DBer.getCreateSql(table)
	if err != nil {
		return nil, nil, err
	}
	sqls = append(sqls, sql)
	if table.extrasql != nil && len(table.extrasql) > 0 {
		size := len(table.extrasql)
		for i := 0; i < size; i++ {
			sqls = append(sqls, table.extrasql[i])
		}
	}
	return sqls, table.extrasql, nil
}

func (d *DBsql) SyscTable(table *db_table) error {
	email := ""
	if err := d.db.Get(&email, "SELECT count(1)||'' FROM USER_TABLES WHERE TABLE_NAME = 'PDA_PHONEFLAG_KEY'"); err != nil {
		fmt.Println("fial:", err)
	}
	fmt.Println(email)

	cntstr := ""
	if err := d.db.Get(&cntstr, d.DBer.DBCheckTableSql(table.name)); err != nil {
		return errors.New("sql:" + d.DBer.DBCheckTableSql(table.name) + ",err:" + err.Error())
	}
	fmt.Println("  cntstr:" + cntstr)
	cnt, _ := strconv.Atoi(cntstr)
	if cnt == 0 {
		sql, err := d.DBer.getCreateSql(table)
		fmt.Println(sql)
		if err != nil {
			return err
		}
		fmt.Printf("%s \n", sql)
		if _, e := d.db.Exec(sql); e != nil {
			return e
		}
	} else {
		db_columns := []string{}
		sql := d.DBer.DBGetColumnsSql(table.name)
		rows, err := d.db.Query(sql)
		fmt.Println(sql)
		defer rows.Close()
		if err != nil {
			return err
		}
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				return err
			}
			db_columns = append(db_columns, name)
		}
		err = rows.Err()
		if err != nil {
			return err
		}

		init_columns := []*db_column{}

		for _, col := range table.cols {
			flag := false
			for _, c2 := range db_columns {
				if strings.Compare(strings.ToLower(col.name), strings.ToLower(c2)) == 0 {
					flag = true
					break
				}
			}
			if !flag {
				init_columns = append(init_columns, col)
			}
		}
		for _, col := range init_columns {
			sql := d.DBer.getAddColumnSql(table, col)
			_, err = d.db.Exec(sql)
			fmt.Println(sql)
			if err != nil {
				return err
			}
		}
	}
	if table.extrasql != nil && len(table.extrasql) > 0 {
		for _, sql := range table.extrasql {
			if _, err := d.db.Exec(sql); err != nil {
				fmt.Errorf(sql)
			}
		}
	}
	return nil
}

func (d *DBsql) Model2Table(model interface{}) (*db_table, error) {

	val := reflect.ValueOf(model)
	field := reflect.Indirect(val)
	typ := reflect.Indirect(val).Type()
	if val.Kind() != reflect.Ptr {
		return nil, errors.New(fmt.Sprintf(" cannot use non-ptr model struct `%s`", getFullName(typ)))
	}
	if typ.Kind() == reflect.Ptr {
		return nil, errors.New(fmt.Sprintf(" only allow ptr model struct, it looks you use two reference to the struct `%s`", typ))
	}
	table := getTableName(val)
	extrasqls := getExtraSql(val)
	cols := []*db_column{}
	tableobj := &db_table{
		table,
		cols,
		extrasqls,
	}
	d.loadColumnInfo(tableobj, field)
	fmt.Printf("%v", cols)
	/**
	for i := 0; i < typ.NumField(); i++ {
		fi := typ.Field(i)
		a, b := parseStructTag(fi.Tag.Get(defaultStructTagName))

		if a["-"] {
			continue
		}

		col_def := b["default"]
		col_haddef := false
		if len(col_def) > 0 {
			col_haddef = true
		}

		col_type := b["type"]
		if len(col_type) == 0 {
			switch fi.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				cod_size := 10
				if size, err := strconv.Atoi(b["size"]); err != nil {
					cod_size = size
				}
				if cod_size <= 0 {
					cod_size = 10
				}
				styp := d.DBer.getType(fi.Type)
				col_type = fmt.Sprintf(styp, cod_size)
			case reflect.Float32, reflect.Float64:
				cod_digits := 10
				if digits, err := strconv.Atoi(b["digits"]); err != nil {
					cod_digits = digits
				}
				if cod_digits <= 0 {
					cod_digits = 10
				}

				cod_decimals := 2
				if decimals, err := strconv.Atoi(b["decimals"]); err != nil {
					cod_decimals = decimals
				}
				if cod_decimals <= 0 {
					cod_decimals = 2
				}
				styp := d.DBer.getType(fi.Type)
				col_type = fmt.Sprintf(styp, cod_digits, cod_decimals)
			case reflect.String:
				cod_size := 80
				if size, err := strconv.Atoi(b["size"]); err != nil {
					cod_size = size
				}
				if cod_size <= 0 {
					cod_size = 80
				}
				styp := d.DBer.getType(fi.Type)
				col_type = fmt.Sprintf(styp, cod_size)
			}

		}
		col_name := b["column"]
		if len(col_name) == 0 {
			col_name = fi.Name
		}
		col_pk := a["pk"]
		col_index := a["index"]
		col_unique := a["unique"]
		col_notnull := a["notnull"]
		cols = append(cols, &db_column{
			name:       col_name,
			dbtype:     col_type,
			defval:     col_def,
			hasdef:     col_haddef,
			primaryKey: col_pk,
			index:      col_index,
			unique:     col_unique,
			notnull:    col_notnull,
		})
	}
	**/
	return tableobj, nil
}

func (d *DBsql) loadColumnInfo(tableobj *db_table, fv reflect.Value) {
	var (
		fi reflect.StructField
	)
	for i := 0; i < fv.NumField(); i++ {
		field := fv.Field(i)
		fi = fv.Type().Field(i)
		a, b := parseStructTag(fi.Tag.Get(defaultStructTagName))

		if a["-"] {
			continue
		}

		if fi.Anonymous {
			d.loadColumnInfo(tableobj, field)
			continue
		}

		col_def := b["default"]
		col_haddef := false
		if len(col_def) > 0 {
			col_haddef = true
		}

		col_type := b["type"]
		if len(col_type) == 0 {
			switch fi.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				cod_size := 10
				if size, err := strconv.Atoi(b["size"]); err != nil {
					cod_size = size
				}
				if cod_size <= 0 {
					cod_size = 10
				}
				styp := d.DBer.getType(fi.Type)
				col_type = fmt.Sprintf(styp, cod_size)
			case reflect.Float32, reflect.Float64:
				cod_digits := 10
				if digits, err := strconv.Atoi(b["digits"]); err != nil {
					cod_digits = digits
				}
				if cod_digits <= 0 {
					cod_digits = 10
				}

				cod_decimals := 2
				if decimals, err := strconv.Atoi(b["decimals"]); err != nil {
					cod_decimals = decimals
				}
				if cod_decimals <= 0 {
					cod_decimals = 2
				}
				styp := d.DBer.getType(fi.Type)
				col_type = fmt.Sprintf(styp, cod_digits, cod_decimals)
			case reflect.String:
				cod_size := 80
				if size, err := strconv.Atoi(b["size"]); err != nil {
					cod_size = size
				}
				if cod_size <= 0 {
					cod_size = 80
				}
				styp := d.DBer.getType(fi.Type)
				col_type = fmt.Sprintf(styp, cod_size)
			}

		}
		col_name := b["column"]
		if len(col_name) == 0 {
			col_name = fi.Name
		}
		col_pk := a["pk"]
		col_index := a["index"]
		col_unique := a["unique"]
		col_notnull := a["notnull"]
		tableobj.cols = append(tableobj.cols, &db_column{
			name:       col_name,
			dbtype:     col_type,
			defval:     col_def,
			hasdef:     col_haddef,
			primaryKey: col_pk,
			index:      col_index,
			unique:     col_unique,
			notnull:    col_notnull,
		})
	}
}

//-------------------------------------
//func (d *DBsql) SyncSqls(models ...interface{}) ([]string, error) {
//	var err error
//	if len(models) == 0 {
//		return nil, errors.New(" not exists models , invalid argument ! ")
//	}
//	sqls := make([]string, len(models))
//	for index, model := range models {
//		if sqls[index], err = d.SyncSql(model); err != nil {
//			return nil, err
//		}
//	}
//	return sqls, nil
//}
//
//func (d *DBsql) SyncSql(model interface{}) (string, error) {
//	if model == nil {
//		return "", errors.New(" not exists models , invalid argument ! ")
//	}
//	table, err := d.m2t(model)
//	if err != nil {
//		return "", err
//	}
//	var sql = ""
//	if sql, err = d.DBer.getCreateSql(table); err != nil {
//		return "", err
//	}
//	return sql, nil
//}
