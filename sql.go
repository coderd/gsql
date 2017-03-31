package gsql

import (
	"database/sql"
)

type DB struct {
	stdDB *sql.DB
}

func NewDB(driverName, dataSourceName string) (*DB, error) {
	stdDB, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	db := &DB{
		stdDB: stdDB,
	}

	return db, nil
}

func (db *DB) ExecReturningRows(query Queryer) ([]map[string]interface{}, error) {
	queryString := query.String()
	args := query.Args()

	stmt, err := db.stdDB.Prepare(queryString)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	return db.formatRows(rows)
}

func (db *DB) ExecWithoutReturningRows(query Queryer) (sql.Result, error) {
	queryString := query.String()
	args := query.Args()

	stmt, err := db.stdDB.Prepare(queryString)
	if err != nil {
		return nil, err
	}

	return stmt.Exec(args...)
}

func (db *DB) formatRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnCount := len(columns)
	scanArgs := make([]interface{}, columnCount)
	values := make([]interface{}, columnCount)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	formattedRows := []map[string]interface{}{}
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		formattedRow := make(map[string]interface{})
		for i, value := range values {
			formattedRow[columns[i]] = value
		}
		formattedRows = append(formattedRows, formattedRow)
	}

	return formattedRows, nil
}
