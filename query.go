package gsql

import (
	"fmt"
	"strings"
	"sync"
)

const (
	actionSelect = 1
	actionInsert = 2
	actionUpdate = 3
	actionDelete = 4
)

type Queryer interface {
	String() string
	Args() []interface{}
}

func NewRawQuery(queryString string, args ...interface{}) *RawQuery {
	return &RawQuery{
		queryString: queryString,
		args:        args,
	}
}

type RawQuery struct {
	queryString string
	args        []interface{}
}

func (rq *RawQuery) String() string {
	return rq.queryString
}

func (rq *RawQuery) Args() []interface{} {
	return rq.args
}

type processedQuery struct {
	whereExpr string
	whereArgs []interface{}
	valueExpr string
	valueArgs []interface{}
	orderExpr string

	queryString string
	args        []interface{}
}

type whereItem struct {
	isAnd  bool
	column string
	op     string
	value  interface{}
}

type Query struct {
	action       int
	table        string
	columns      []string
	wheres       []*whereItem
	orders       map[string]string
	offset       int
	rowCount     int
	insertValues map[string]interface{}
	updateValues map[string]interface{}
	returnType   int

	isProcessed bool
	processMu   *sync.Mutex
	processed   *processedQuery
}

func NewQuery() *Query {
	return &Query{
		wheres:    []*whereItem{},
		processed: &processedQuery{},
		processMu: &sync.Mutex{},
	}
}

func (q *Query) Table(table string) *Query {
	q.table = table

	return q
}

func (q *Query) Where(column, op string, value interface{}) *Query {
	q.wheres = append(
		q.wheres,
		&whereItem{
			isAnd:  true,
			column: column,
			op:     op,
			value:  value,
		})

	return q
}

func (q *Query) OrWhere(column, op string, value interface{}) *Query {
	q.wheres = append(
		q.wheres,
		&whereItem{
			isAnd:  false,
			column: column,
			op:     op,
			value:  value,
		})

	return q
}

func (q *Query) OrderBy(orders map[string]string) *Query {
	q.orders = orders

	return q
}

func (q *Query) Limit(offset, rowCount int) *Query {
	q.offset = offset
	q.rowCount = rowCount

	return q
}

func (q *Query) Select(columns []string) *Query {
	if len(columns) == 0 {
		panic("gsql: Query.Select() method requires at least 1 column, got 0")
	}

	q.action = actionSelect
	q.columns = columns

	return q
}

func (q *Query) Insert(values map[string]interface{}) *Query {
	if len(values) == 0 {
		panic("gsql: Query.Insert() method requires at least 1 value, got 0")
	}

	q.action = actionInsert
	q.insertValues = values

	return q
}

func (q *Query) Update(values map[string]interface{}) *Query {
	if len(values) == 0 {
		panic("gsql: Query.Update() method requires at least 1 value, got 0")
	}

	q.action = actionUpdate
	q.updateValues = values

	return q
}

func (q *Query) Delete() *Query {
	q.action = actionDelete

	return q
}

func (q *Query) String() string {
	q.process()

	return q.processed.queryString
}

func (q *Query) Args() []interface{} {
	q.process()

	return q.processed.args
}

func (q *Query) process() {
	q.processMu.Lock()
	defer q.processMu.Unlock()
	if q.isProcessed {
		return
	}

	switch q.action {
	case actionSelect:
		q.processSelect()
	case actionInsert:
		q.processInsert()
	case actionUpdate:
		q.processUpdate()
	case actionDelete:
		q.processDelete()
	default:
		panic("gsql: you must call one of these methods: Query.[Select(), Insert(), Update(), Delete()] first")
	}

	q.isProcessed = true
}

func (q *Query) processSelect() {
	columns := []string{}
	for _, column := range q.columns {
		columns = append(columns, "`"+column+"`")
	}

	q.processWheres()
	q.processOrders()

	queryString := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), q.table)
	if q.processed.whereExpr != "" {
		queryString = queryString + " WHERE " + q.processed.whereExpr
	}
	if q.processed.orderExpr != "" {
		queryString = queryString + " ORDER BY " + q.processed.orderExpr
	}

	if q.rowCount > 0 {
		queryString = queryString + " LIMIT " + fmt.Sprintf("%d, %d", q.offset, q.rowCount)
	}

	q.processed.queryString = queryString
	q.processed.args = q.processed.whereArgs
}

func (q *Query) processInsert() {
	q.processInsertValues()

	q.processed.queryString = "INSERT INTO `" + q.table + "` " + q.processed.valueExpr
	q.processed.args = q.processed.valueArgs
}

func (q *Query) processUpdate() {
	q.processUpdateValues()
	q.processWheres()

	queryString := "UPDATE `" + q.table + "` " + q.processed.valueExpr
	if q.processed.whereExpr != "" {
		queryString = queryString + " WHERE " + q.processed.whereExpr
	}
	if q.processed.orderExpr != "" {
		queryString = queryString + " ORDER BY " + q.processed.orderExpr
	}
	if q.rowCount > 0 {
		queryString = queryString + " LIMIT " + fmt.Sprintf("%d", q.rowCount)
	}

	q.processed.queryString = queryString
	if len(q.processed.valueArgs) > 0 {
		q.processed.args = append(q.processed.args, q.processed.valueArgs...)
	}
	if len(q.processed.whereArgs) > 0 {
		q.processed.args = append(q.processed.args, q.processed.whereArgs...)
	}
}

func (q *Query) processDelete() {
	q.processWheres()

	queryString := "DELETE FROM " + q.table
	if q.processed.whereExpr != "" {
		queryString = queryString + " WHERE " + q.processed.whereExpr
	}
	if q.processed.orderExpr != "" {
		queryString = queryString + " ORDER BY " + q.processed.orderExpr
	}
	if q.rowCount > 0 {
		queryString = queryString + " LIMIT " + fmt.Sprintf("%d", q.rowCount)
	}

	q.processed.queryString = queryString
	q.processed.args = q.processed.whereArgs
}

func (q *Query) processWheres() {
	wheresLen := len(q.wheres)
	if wheresLen == 0 {
		return
	}

	args := make([]interface{}, 0, wheresLen)
	for i, item := range q.wheres {
		var expr string
		if item.op == "IN" {
			v, ok := item.value.([]interface{})
			if !ok {
				panic("gsql: the value of 'IN' operator should be type of []interface{}")
			}
			vLen := len(v)
			expr = "`" + item.column + "` IN (" + strings.TrimLeft(strings.Repeat(", ?", vLen), ", ") + ")"
			args = append(args, v...)
		} else {
			expr = "`" + item.column + "` " + item.op + " ?"
			args = append(args, item.value)
		}
		if i > 0 {
			if item.isAnd {
				expr = "AND " + expr
			} else {
				expr = "OR " + expr
			}
		}
		q.processed.whereExpr += " " + expr
	}

	q.processed.whereArgs = args
}

func (q *Query) processInsertValues() {
	valueCount := len(q.insertValues)
	if valueCount == 0 {
		return
	}

	columns := make([]string, 0, valueCount)
	markers := make([]string, 0, valueCount)
	args := make([]interface{}, 0, valueCount)
	for column, value := range q.insertValues {
		columns = append(columns, "`"+column+"`")
		markers = append(markers, "?")
		args = append(args, value)
	}

	q.processed.valueExpr = "(" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(markers, ", ") + ")"
	q.processed.valueArgs = args
}

func (q *Query) processUpdateValues() {
	valueCount := len(q.updateValues)
	if valueCount == 0 {
		return
	}

	args := make([]interface{}, 0, valueCount)
	kvs := make([]string, 0, valueCount)
	for column, value := range q.updateValues {
		kvs = append(kvs, "`"+column+"` = ?")
		args = append(args, value)
	}

	q.processed.valueExpr = "SET " + strings.Join(kvs, ", ")
	q.processed.valueArgs = args
}

func (q *Query) processOrders() {
	if len(q.orders) == 0 {
		return
	}

	orders := []string{}
	for column, direction := range q.orders {
		orders = append(orders, "`"+column+"` "+strings.ToUpper(direction))
	}

	q.processed.orderExpr = strings.Join(orders, ", ")
}
