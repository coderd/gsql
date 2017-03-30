package gsql

const (
	returnTypeRows        = 1
	returnTypeWithoutRows = 2
)

type Query interface {
	String() string
	Args() []interface{}
	ReturnType() int
}

type query struct {
	table      string
	columns    []string
	wheres     map[string]interface{}
	orders     map[string]string
	offset     int
	rowCount   int
	values     map[string]interface{}
	returnType int
}

func NewQuery() *query {
	return &query{}
}

func (q *query) Table(table string) *query {
	q.table = table

	return q
}

func (q *query) Where(wheres map[string]interface{}) *query {
	q.wheres = wheres

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

func (q *query) Select(columns []string) {
	q.returnType = returnTypeRows

	q.columns = columns
}

func (q *query) Insert(values map[string]interface{}) {
	q.returnType = returnTypeWithoutRows

	q.values = values
}

func (q *query) Update(values map[string]interface{}) {
	q.returnType = returnTypeWithoutRows

	q.values = values
}

func (q *query) Delete() {
	q.returnType = returnTypeWithoutRows
}

func (q *query) String() string {
	return "select number, square_number from square_num"
}

func (q *query) Args() []interface{} {
	return []interface{}{}
}

func (q *query) ReturnType() int {
	return returnTypeRows
}
