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

type Query interface {
	String() string
	Args() []interface{}
}

type query struct {
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

func NewQuery() *query {
	return &query{
		wheres:    []*whereItem{},
		processed: &processedQuery{},
		processMu: &sync.Mutex{},
	}
}

func (q *query) Table(table string) *query {
	q.table = table

	return q
}

func (q *query) Where(column, op string, value interface{}) *query {
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

func (q *query) OrWhere(column, op string, value interface{}) *query {
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

func (q *query) OrderBy(orders map[string]string) *query {
	q.orders = orders

	return q
}

func (q *query) Limit(offset, rowCount int) *query {
	q.offset = offset
	q.rowCount = rowCount

	return q
}

func (q *query) Select(columns []string) *query {
	if len(columns) == 0 {
		panic("gsql: Query.Select() method requires at least 1 column, got 0")
	}

	q.action = actionSelect
	q.columns = columns

	return q
}

func (q *query) Insert(values map[string]interface{}) *query {
	if len(values) == 0 {
		panic("gsql: Query.Insert() method requires at least 1 value, got 0")
	}

	q.action = actionInsert
	q.insertValues = values

	return q
}

func (q *query) Update(values map[string]interface{}) *query {
	if len(values) == 0 {
		panic("gsql: Query.Update() method requires at least 1 value, got 0")
	}

	q.action = actionUpdate
	q.updateValues = values

	return q
}

func (q *query) Delete() *query {
	q.action = actionDelete

	return q
}

func (q *query) String() string {
	q.process()

	return q.processed.queryString
}

func (q *query) Args() []interface{} {
	q.process()

	return q.processed.args
}

func (q *query) process() {
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

func (q *query) processSelect() {
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

func (q *query) processInsert() {
	q.processInsertValues()

	q.processed.queryString = "INSERT INTO `" + q.table + "` " + q.processed.valueExpr
	q.processed.args = q.processed.valueArgs
}

func (q *query) processUpdate() {
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

func (q *query) processDelete() {
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

func (q *query) processWheres() {
	wheresLen := len(q.wheres)
	if q.wheres == nil || wheresLen == 0 {
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

func (q *query) processInsertValues() {
	valueCount := len(q.insertValues)
	if q.insertValues == nil || valueCount == 0 {
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

func (q *query) processUpdateValues() {
	valueCount := len(q.updateValues)
	if q.updateValues == nil || valueCount == 0 {
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

func (q *query) processOrders() {
	if len(q.orders) == 0 {
		return
	}

	orders := []string{}
	for column, direction := range q.orders {
		orders = append(orders, "`"+column+"` "+strings.ToUpper(direction))
	}

	q.processed.orderExpr = strings.Join(orders, ", ")
}
