package gsql

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

/*
CREATE DATABASE IF NOT EXISTS `test_gsql` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci
create user 'gsql_rw'@'%' identified by '1'
grant all privileges on *.* to 'gsql_rw'@'%'
flush privileges

CREATE TABLE `user` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `email` varchar(128) NOT NULL,
  `name` varchar(128) NOT NULL DEFAULT '',
  `status` tinyint(1) NOT NULL DEFAULT '0',
  `updated_at` int(11) unsigned NOT NULL,
  `created_at` int(11) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

*/

func TestSelect(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	q := NewQuery()
	q.Table("user")
	q.Select([]string{
		"id",
		"email",
		"name",
		"status",
		"updated_at",
		"created_at",
	})
	q.Where("id", "<", 100000)
	q.Where("status", "=", 0)
	q.OrderBy(map[string]string{
		"id": "DESC",
	})
	q.Limit(0, 10)

	_, err = db.ExecReturningRows(q)
	assert.Nil(t, err)
}

func randomEmail() string {
	return fmt.Sprintf("%d@gsql.com", time.Now().UnixNano())
}

func TestInsert(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	q := NewQuery()
	q.Table("user")
	nowUnix := time.Now().Unix()
	q.Insert(map[string]interface{}{
		"email":      randomEmail(),
		"name":       "",
		"updated_at": nowUnix,
		"created_at": nowUnix,
	})

	result, err := db.ExecWithoutReturningRows(q)
	assert.Nil(t, err)

	lastInsertId, err := result.LastInsertId()
	assert.Nil(t, err)
	assert.NotZero(t, lastInsertId)

	rowsAffected, err := result.RowsAffected()
	assert.Nil(t, err)
	var expected int64 = 1
	assert.Equal(t, expected, rowsAffected)
}

func TestUpdate(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	q := NewQuery()
	q.Table("user")
	nowUnix := time.Now().Unix()
	q.Where("id", ">", 1)
	q.Update(map[string]interface{}{
		"status":     1,
		"updated_at": nowUnix,
	})
	q.OrderBy(map[string]string{
		"id": "DESC",
	})
	q.Limit(0, 2)

	result, err := db.ExecWithoutReturningRows(q)
	assert.Nil(t, err)

	lastInsertId, err := result.LastInsertId()
	assert.Nil(t, err)
	assert.Zero(t, lastInsertId)

	rowsAffected, err := result.RowsAffected()
	assert.Nil(t, err)
	assert.NotZero(t, rowsAffected)
}

func TestDelete(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	q := NewQuery()
	q.Table("user")
	q.Delete()
	q.Where("id", ">", 1)
	q.OrderBy(map[string]string{
		"id": "DESC",
	})
	q.Limit(0, 1)

	result, err := db.ExecWithoutReturningRows(q)
	assert.Nil(t, err)

	lastInsertId, err := result.LastInsertId()
	assert.Nil(t, err)
	assert.Zero(t, lastInsertId)

	rowsAffected, err := result.RowsAffected()
	assert.Nil(t, err)
	var expected int64 = 1
	assert.Equal(t, expected, rowsAffected)
}

func TestRawQuerySelect(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	rq := NewRawQuery("SELECT `id`, `email`, `name`, `status`, `updated_at`, `created_at` FROM `user` where `id` < ? AND `status` = ? ORDER BY `id` DESC LIMIT 0, 10", 100000, 0)
	_, err = db.ExecReturningRows(rq)
	assert.Nil(t, err)
}

func TestRawQueryInsert(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	nowUnix := time.Now().Unix()
	rq := NewRawQuery("INSERT INTO `user` (`email`, `name`, `updated_at`, `created_at`) VALUES (?, ?, ?, ?)", randomEmail(), "", nowUnix, nowUnix)
	result, err := db.ExecWithoutReturningRows(rq)
	assert.Nil(t, err)

	lastInsertId, err := result.LastInsertId()
	assert.Nil(t, err)
	assert.NotZero(t, lastInsertId)

	rowsAffected, err := result.RowsAffected()
	assert.Nil(t, err)
	var expected int64 = 1
	assert.Equal(t, expected, rowsAffected)
}

func TestRawQueryUpdate(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	nowUnix := time.Now().Unix()
	rq := NewRawQuery("UPDATE `user` SET `status` = ?, `updated_at` = ? WHERE `id` > ? ORDER BY `id` DESC LIMIT 2", 1, 1, nowUnix)
	result, err := db.ExecWithoutReturningRows(rq)
	assert.Nil(t, err)

	lastInsertId, err := result.LastInsertId()
	assert.Nil(t, err)
	assert.Zero(t, lastInsertId)

	_, err = result.RowsAffected()
	assert.Nil(t, err)
}

func TestRawQueryDelete(t *testing.T) {
	db, err := NewDB("mysql", "gsql_rw:1@/test_gsql")
	if err != nil {
		t.Error(err)
	}

	rq := NewRawQuery("DELETE FROM `user` WHERE `id` > ? ORDER BY `id` DESC LIMIT 1", 1)
	result, err := db.ExecWithoutReturningRows(rq)
	assert.Nil(t, err)

	lastInsertId, err := result.LastInsertId()
	assert.Nil(t, err)
	assert.Zero(t, lastInsertId)

	rowsAffected, err := result.RowsAffected()
	assert.Nil(t, err)
	var expected int64 = 1
	assert.Equal(t, expected, rowsAffected)
}
