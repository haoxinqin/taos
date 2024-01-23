package util

import (
	"database/sql"
	"log"

	_ "github.com/taosdata/driver-go/v3/taosSql"

	"fmt"
)

type Taos struct {
	taos        *sql.DB
	statement   *statement
	config      *map[string]interface{}
	show_params bool
	Error       error
	clone       int
}

func CreateTaos(config *map[string]interface{}) (taos *Taos, err error) {
	taos = &Taos{
		statement:   &statement{},
		show_params: (*config)["show_params"].(bool),
		config:      config,
		clone:       1,
	}
	conn, err := taos.CreateConn((*config)["user"].(string), (*config)["password"].(string), (*config)["addr"].(string), (*config)["dbname"].(string), (*config)["port"].(int))
	if err != nil {
		log.Panicln("创建taos链接失败====", err.Error())
		return
	}
	err = conn.Ping()
	if err != nil {
		log.Panicln("taos链接失败===", err.Error())
	}
	log.Println("taos连接成功")
	taos.taos = conn
	return
}

// 创建数据库链接
func (t *Taos) CreateConn(user, password, addr, dbname string, port int) (conn *sql.DB, err error) {
	taos_uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, addr, port, dbname)
	conn, err = sql.Open("taosSql", taos_uri)
	return
}

// 表格
func (t *Taos) Table(table string, args ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Table = table
	return
}

// 超级表
func (t *Taos) Stable(stable string, tags ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Stable = stable
	tx.statement.Tags = tags
	return
}

// where子句
func (t *Taos) Where(query interface{}, args ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.BuildCondition(query, args...)
	return
}

// group子句
func (t *Taos) Group(group string) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Group = group
	return
}

// order子句
func (t *Taos) Order(order string) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Order = order
	return
}

// limit子句
func (t *Taos) Limit(limit int) (tx *Taos) {
	tx = t.getInstance()
	if limit < 0 {
		limit = 0
	}
	tx.statement.Limit = limit
	return
}

// offset子句
func (t *Taos) Page(page int) (tx *Taos) {
	tx = t.getInstance()
	if page <= 1 {
		page = 1
	}
	tx.statement.Page = page
	return
}

// field子句
func (t *Taos) Field(field string) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Selects = append(tx.statement.Selects, field)
	return
}

// 执行查询,查询多条数据
func (t *Taos) Find(dest interface{}, args ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Execute(dest, args...)
	return
}

// 查询单挑数据
func (t *Taos) Take(dest interface{}, args ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.Page = 1
	tx.statement.Limit = 1
	tx.statement.Execute(dest, args...)
	return
}

// 统计
func (t *Taos) Count(total *int64, args ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.ExecuteCount(total, args...)
	return
}

// 执行sql语句
func (t *Taos) Exec(sql string, args ...interface{}) (tx *Taos) {
	tx = t.getInstance()
	_, err := tx.taos.Exec(sql, args...)
	tx.Error = err
	// log.Println(result)
	return
}

// 插入数据
func (t *Taos) Insert(data *map[string]interface{}) (tx *Taos) {
	tx = t.getInstance()
	tx.statement.ExecuteInsert(data)
	return
}

// 获取一个实例
func (t *Taos) getInstance() *Taos {
	// conn_err := t.taos.Ping()
	// if conn_err != nil {
	// 	config := t.config
	// 	t.CreateConn((*config)["user"].(string), (*config)["password"].(string), (*config)["addr"].(string), (*config)["dbname"].(string), (*config)["port"].(int))
	// 	log.Println("taos链接丢失")
	// }
	if t.clone > 0 {
		tx := &Taos{
			taos: t.taos,
		}
		if t.clone == 1 {
			tx.statement = &statement{
				Db: tx,
			}
		} else {
			tx.statement = t.statement.clone()
			tx.statement.Db = tx
		}
		return tx
	}
	return t
}
