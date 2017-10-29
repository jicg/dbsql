package dbsql

import (
	"reflect"
	"strings"
	"fmt"
)

var supportTag = map[string]int{
	"-":        1,
	"notnull":  1,
	"index":    1,
	"unique":   1,
	"pk":       1,
	"size":     2,
	"column":   2,
	"default":  2,
	"type":     2,
	"digits":   2,
	"decimals": 2,
}

func getTableName(val reflect.Value) string {
	if fun := val.MethodByName("TableName"); fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		if len(vals) > 0 && vals[0].Kind() == reflect.String {
			return vals[0].String()
		}
	}
	return reflect.Indirect(val).Type().Name()
}

func getFullName(typ reflect.Type) string {
	return typ.PkgPath() + "." + typ.Name()
}

func parseStructTag(data string) (attrs map[string]bool, tags map[string]string) {
	attrs = make(map[string]bool)
	tags = make(map[string]string)
	for _, v := range strings.Split(data, defaultStructTagDelim) {
		if v == "" {
			continue
		}
		v = strings.TrimSpace(v)
		if t := strings.ToLower(v); supportTag[t] == 1 {
			attrs[t] = true
		} else if i := strings.Index(v, "("); i > 0 && strings.LastIndex(v, ")") == len(v)-1 {
			name := t[:i]
			if supportTag[name] == 2 {
				v = v[i+1: len(v)-1]
				tags[name] = v
			}
		} else {
			fmt.Println("unsupport orm tag", v)
		}
	}
	return
}
