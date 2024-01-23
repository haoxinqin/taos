package util

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
)

type statement struct {
	Db           *Taos
	Where        []string
	Table        string
	Stable       string
	Selects      []string
	Omits        []string
	Group        string
	Order        string
	Limit        int
	Page         int
	Vars         []interface{}
	AffectedRows int64
	Tags         []interface{}
}

func (s *statement) clone() *statement {
	news := &statement{
		Table:   s.Table,
		Where:   make([]string, 0),
		Selects: make([]string, 0),
		Omits:   make([]string, 0),
		Vars:    make([]interface{}, 0),
		Tags:    make([]interface{}, 0),
	}
	return news
}

func (stmt *statement) BuildCondition(query interface{}, args ...interface{}) {
	if s, ok := query.(string); ok {
		if _, err := strconv.Atoi(s); err != nil {
			if s == "" && len(args) == 0 {
				return
			}
			if strings.Contains(s, "?") {
				if strings.Count(s, "?") != len(args) {
					return
				} else {
					stmt.Where = append(stmt.Where, s)
					// 如果参数中有切片，转成字符串
					for _, arg := range args {
						if reflect.TypeOf(arg).Kind() == reflect.Slice {

							larg := reflect.ValueOf(arg).Len()
							if larg > 0 {
								var varstr strings.Builder
								varstr.WriteString("(")
								for i := 0; i < larg; i++ {
									element := reflect.ValueOf(arg).Index(i)
									if element.Kind() == reflect.String {
										varstr.WriteString("'")
										varstr.WriteString(element.String())
										varstr.WriteString("'")
									} else {
										varstr.WriteString(fmt.Sprintf("%v", element.Interface()))
									}
									if i == larg-1 {
										varstr.WriteString(")")
									} else {
										varstr.WriteString(", ")
									}
								}
								stmt.Vars = append(stmt.Vars, varstr.String())
							}
							continue
						}
						if reflect.TypeOf(arg).Kind() == reflect.String {
							stmt.Vars = append(stmt.Vars, fmt.Sprintf("'%v'", reflect.ValueOf(arg).String()))
							continue
						}
						stmt.Vars = append(stmt.Vars, arg)
					}
				}
			} else {
				if s == "" {
					return
				}
			}
		}
	}
	if m, ok := query.(map[string]interface{}); ok {
		if len(m) == 0 {
			return
		}
		var str strings.Builder
		for mk, mv := range m {
			// 判断字符串
			if reflect.TypeOf(mv).Kind() == reflect.String {
				// 如果是字符串需要加上单引号
				str.WriteString(fmt.Sprintf("%s = '%v'", mk, mv))
			} else {
				str.WriteString(fmt.Sprintf("%s = %v", mk, mv))
			}
			str.WriteString(" and ")
		}
		wherestr := str.String()
		if len(wherestr) > 0 {
			wherestr = wherestr[:len(wherestr)-len(" and ")]
		}
		stmt.Where = append(stmt.Where, wherestr)
	}
}

func (stmt *statement) ExecuteInsert(data *map[string]interface{}) {
	sql, err := stmt.BuildInsert(data)
	if err != nil {
		stmt.Db.Error = err
	}
	// 做个判断sql语句中的?,要和参数数量相同
	if strings.Count(sql, "?") != len(stmt.Vars) {
		stmt.Db.Error = errors.New("参数配置错误")
		return
	}
	if stmt.Db.show_params {
		log.Println("sql========", sql, stmt.Vars)
	} else {
		has_params_sql := sql
		for _, v := range stmt.Vars {
			// log.Println(v)
			has_params_sql = strings.Replace(has_params_sql, "?", fmt.Sprintf("%v", v), 1)
		}
		log.Println(has_params_sql)
	}
	ctx := context.Background()
	conn, err := stmt.Db.taos.Conn(ctx)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	defer conn.Close()
	result, err := conn.ExecContext(ctx, sql, stmt.Vars...)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	num, err := result.RowsAffected()
	if err != nil {
		stmt.Db.Error = err
		return
	}
	stmt.AffectedRows = num
}

func (stmt *statement) BuildInsert(data *map[string]interface{}) (sql string, err error) {
	if stmt.Table == "" {
		err = errors.New("请指定表格")
		return
	}
	var (
		sql_builder  strings.Builder
		sql_builder1 strings.Builder
	)
	sql_builder.WriteString("INSERT  INTO  ")
	sql_builder.WriteString(stmt.Table)
	if stmt.Stable != "" {
		// 如果是自动建表，必须要有tags
		if len(stmt.Tags) == 0 {
			err = errors.New("请指定超级表tags")
			return
		}
		sql_builder.WriteString(" USING  ")
		sql_builder.WriteString(stmt.Stable)
		sql_builder.WriteString(" TAGS(")
		tag1 := make([]string, 0)
		for _, tag := range stmt.Tags {
			tag1 = append(tag1, "?")
			if reflect.TypeOf(tag).Kind() == reflect.String {
				stmt.Vars = append(stmt.Vars, "'"+tag.(string)+"'")
			} else {
				stmt.Vars = append(stmt.Vars, tag)
			}
		}
		sql_builder.WriteString(strings.Join(tag1, ", "))
		sql_builder.WriteString(")")
	}

	sql_builder.WriteString(" ( ")
	sql_builder1.WriteString(" ( ")

	keys := make([]string, 0)
	valus := make([]string, 0)

	for k, v := range *data {
		keys = append(keys, k)
		valus = append(valus, "?")
		if reflect.TypeOf(v).Kind() == reflect.String {
			stmt.Vars = append(stmt.Vars, "'"+v.(string)+"'")
		} else {
			stmt.Vars = append(stmt.Vars, v)
		}
	}
	sql_builder.WriteString(strings.Join(keys, ", "))
	sql_builder1.WriteString(strings.Join(valus, ", "))
	sql_builder.WriteString(") VALUES ")
	sql_builder1.WriteString(")")
	sql_builder.WriteString(sql_builder1.String())
	sql = sql_builder.String()

	return
}

// 创建select语句
func (stmt *statement) BuildSelect() (sql string, err error) {
	if stmt.Table == "" {
		err = errors.New("table是必须的")
		return
	}
	var sql_builder strings.Builder
	sql_builder.WriteString("select")
	if len(stmt.Selects) == 0 {
		sql_builder.WriteString(" * ")
	} else {
		sql_builder.WriteString(" ")
		sql_builder.WriteString(strings.Join(stmt.Selects, ","))
	}
	sql_builder.WriteString(" from ")
	sql_builder.WriteString(stmt.Table)
	lwhere := len(stmt.Where)
	if lwhere > 0 {
		sql_builder.WriteString(" where ")
		for k, v := range stmt.Where {
			sql_builder.WriteString("(")
			sql_builder.WriteString(v)
			sql_builder.WriteString(")")
			if k < lwhere-1 {
				sql_builder.WriteString(" and ")
			}
		}
	}
	if stmt.Order != "" {
		sql_builder.WriteString(" order by ")
		sql_builder.WriteString(stmt.Order)
	}
	if stmt.Group != "" {
		sql_builder.WriteString(" Group by ")
		sql_builder.WriteString(stmt.Group)
	}
	if stmt.Limit > 0 && stmt.Page > 0 {
		sql_builder.WriteString(fmt.Sprintf(" limit %d, %d ", (stmt.Page-1)*stmt.Limit, stmt.Limit))
	}
	sql = sql_builder.String()
	// log.Println("sql=====", sql)
	return
}

// 创建count语句
func (stmt *statement) BuildCount() (sql string, err error) {
	if stmt.Table == "" {
		err = errors.New("table是必须的")
		return
	}
	var sql_builder strings.Builder
	sql_builder.WriteString("select")
	if len(stmt.Selects) == 0 {
		sql_builder.WriteString(" count(1) as total ")
	} else {
		sql_builder.WriteString(" ")
		sql_builder.WriteString(strings.Join(stmt.Selects, ","))
	}
	sql_builder.WriteString(" from ")
	sql_builder.WriteString(stmt.Table)
	lwhere := len(stmt.Where)
	if lwhere > 0 {
		sql_builder.WriteString(" where ")
		for k, v := range stmt.Where {
			sql_builder.WriteString("(")
			sql_builder.WriteString(v)
			sql_builder.WriteString(")")
			if k < lwhere-1 {
				sql_builder.WriteString(" and ")
			}
		}
	}
	sql = sql_builder.String()
	return
}

// 执行sql语句
func (stmt *statement) Execute(dest interface{}, args ...interface{}) {
	sql, err := stmt.BuildSelect()
	if err != nil {
		stmt.Db.Error = err
		return
	}
	stmt.Vars = append(stmt.Vars, args...)
	// 做个判断sql语句中的?,要和参数数量相同
	if strings.Count(sql, "?") != len(stmt.Vars) {
		stmt.Db.Error = errors.New("参数配置错误")
		return
	}
	if stmt.Db.show_params {
		log.Println("sql========", sql, stmt.Vars)
	} else {
		has_params_sql := sql
		for _, v := range stmt.Vars {
			log.Println(v)
			has_params_sql = strings.Replace(has_params_sql, "?", fmt.Sprintf("%v", v), 1)
		}
		log.Println(has_params_sql)
	}
	ctx := context.Background()
	conn, err := stmt.Db.taos.Conn(ctx)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, sql, stmt.Vars...)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	stmt.assignRows(rows, dest)
}

// 执行sql语句
func (stmt *statement) ExecuteCount(total *int64, args ...interface{}) {
	sql, err := stmt.BuildCount()
	if err != nil {
		stmt.Db.Error = err
		return
	}
	stmt.Vars = append(stmt.Vars, args...)
	// 做个判断sql语句中的?,要和参数数量相同
	if strings.Count(sql, "?") != len(stmt.Vars) {
		stmt.Db.Error = errors.New("参数配置错误")
		return
	}
	if stmt.Db.show_params {
		log.Println("sql========", sql, stmt.Vars)
	} else {
		has_params_sql := sql
		for _, v := range stmt.Vars {
			has_params_sql = strings.Replace(has_params_sql, "?", fmt.Sprintf("%v", v), 1)
		}
		log.Println(has_params_sql)
	}
	ctx := context.Background()
	conn, err := stmt.Db.taos.Conn(ctx)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, sql, stmt.Vars...)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	stmt.assignCount(rows, total)
}

// 统计
func (stmt *statement) assignCount(rows *sql.Rows, dest *int64) {
	defer rows.Close()
	for rows.Next() {
		values := make([]interface{}, 1)
		// 将指针数组传递给Scan函数
		valuePointers := make([]interface{}, 1)
		for i := range values {
			valuePointers[i] = &values[i]
		}
		// 执行Scan函数
		err := rows.Scan(valuePointers...)
		if err != nil {
			stmt.Db.Error = err
			return
		}
		*dest = values[0].(int64)
	}
}

// 将查询的结构映射到传递过来的容器中
func (stmt *statement) assignRows(rows *sql.Rows, dest interface{}) {
	defer rows.Close()
	// 获取结构体字段信息
	columns, err := rows.Columns()
	if err != nil {
		stmt.Db.Error = err
		return
	}
	list := make([]map[string]interface{}, 0)
	li := make(map[string]interface{})
	// 获取结构体字段个数
	numColumns := len(columns)
	// 遍历查询结果
	for rows.Next() {
		// 创建一个字段值的切片
		values := make([]interface{}, numColumns)
		data := make(map[string]interface{}, 0)
		// 将指针数组传递给Scan函数
		valuePointers := make([]interface{}, numColumns)
		for i := range values {
			valuePointers[i] = &values[i]
		}
		// 执行Scan函数
		err := rows.Scan(valuePointers...)
		if err != nil {
			stmt.Db.Error = err
			return
		}
		for k, value := range values {
			data[columns[k]] = value
		}
		li = data
		list = append(list, data)
	}
	if reflect.TypeOf(dest).Elem().Kind() == reflect.Slice {
		stmt.mapToStruct(list, dest)
	} else {
		stmt.mapToStruct(li, dest)
	}
}

func (stmt *statement) mapToStruct(m interface{}, dest interface{}) {
	listbyte, err := json.Marshal(m)
	if err != nil {
		stmt.Db.Error = err
		return
	}
	err = json.Unmarshal(listbyte, dest)
	if err != nil {
		stmt.Db.Error = err
		return
	}
}
