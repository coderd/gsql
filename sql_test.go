package gsql

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

/*
CREATE DATABASE IF NOT EXISTS `test_gsql` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci
create user 'gsql_rw'@'%' identified by '1'
grant all privileges on *.* to 'gsql_rw'@'%'
flush privileges

CREATE TABLE `square_num` (
  `number` int(11) NOT NULL,
  `square_number` int(11) NOT NULL,
  PRIMARY KEY (`number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

*/

func TestExecReturningRows(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	q := NewQuery()
	_, err = db.ExecReturningRows(q)
	if err != nil {
		t.Error(err)
	}
}

func TestExecWithoutReturningRows(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	q := NewQuery()
	_, err = db.ExecWithoutReturningRows(q)
	if err != nil {
		t.Error(err)
	}
}
