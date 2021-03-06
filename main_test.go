package dbsql

import (
	"fmt"
	"testing"
)

type Obj struct {
	name string  `dbsql:"pk"  `
	pwd  string  `dbsql:"type(varchar2(803))"  `
	qty  int     `dbsql:"notnull"`
	uuid float64 `dbsql:"digits(10);decimals(2);default(0)"  `
}

/*
func Test_main(t *testing.T) {
	db := &dbOracle{}
	s := &DBsql{db, nil}
	sql, err := s.SyncSql(new(Obj))
	if err != nil {
		fmt.Println(err.Error() + " ")
	} else {
		fmt.Println(sql + " ")
	}
}
*/

func Test_abc(t *testing.T) {
	v := []string{"hello world"}
	fmt.Println(typeof(v))
}
func typeof(v interface{}) string {
	return fmt.Sprintf("%T", v)
}
