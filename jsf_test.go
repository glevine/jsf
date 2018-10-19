package jsf_test

import (
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/glevine/jsf"
	"github.com/stretchr/testify/assert"
)

func ExampleApplyFilter() {
	filter := []byte(`[{"$or":[{"first_name":{"$equals":"Tim"}},{"last_name":{"$equals":"Wolf"}},{"home_phone":{"$equals":"919-821-3220"}},{"$and":[{"city":{"$equals":"Chicago"}},{"zip":{"$equals":"12345"}},{"$or":[{"state":{"$equals":"California"}},{"state":{"$equals":"Wisconsin"}},{"$and":[{"postal_code":{"$equals":"21121"}},{"street":{"$equals":"Baker Street"}}]}]}]}]}]`)
	q := sq.Select("first_name", "last_name").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	if err != nil {
		fmt.Println(err)
		return
	}

	sql, args, err := q.ToSql()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// SELECT first_name, last_name FROM db WHERE ((first_name = ? OR last_name = ? OR home_phone = ? OR (city = ? AND zip = ? AND (state = ? OR state = ? OR (postal_code = ? AND street = ?)))))
	// [Tim Wolf 919-821-3220 Chicago 12345 California Wisconsin 21121 Baker Street]
}

func TestEquals(t *testing.T) {
	filter := []byte(`[{"MovieName":{"$equals": "Godzilla"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (MovieName = ?)", sql)
	assert.Equal(t, []interface{}{"Godzilla"}, args)
}

func TestNotEquals(t *testing.T) {
	filter := []byte(`[{"ActressName":{"$not_equals": "Johny"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ActressName <> ?)", sql)
	assert.Equal(t, []interface{}{"Johny"}, args)
}

func TestGreaterThan(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$gt": "2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate > ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestGreaterThanOrEqualTo(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$gte": "2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate >= ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestLessThan(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$lt": "2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate < ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestLessThanOrEqualTo(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$lte": "2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate <= ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestIsNull(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$is_null": true}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, _, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate IS NULL)", sql)
}

func TestIsNotNull(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$not_null": true}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, _, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate IS NOT NULL)", sql)
}

func TestIn(t *testing.T) {
	filter := []byte(`[{"ActressName":{"$in": ["Jamie", "Johnny"]}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ActressName IN (?,?))", sql)
	assert.Equal(t, []interface{}{"Jamie", "Johnny"}, args)
}

func TestNotIn(t *testing.T) {
	filter := []byte(`[{"ActressName":{"$not_in": ["Jamie", "Johnny"]}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ActressName NOT IN (?,?))", sql)
	assert.Equal(t, []interface{}{"Jamie", "Johnny"}, args)
}

func TestAnd(t *testing.T) {
	filter := []byte(`[{"$and":[{"ReleaseDate":{"$equals":"2018-10-18"}},{"Rating":{"$equals":"PG"}}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((ReleaseDate = ? AND Rating = ?))", sql)
	assert.Equal(t, []interface{}{"2018-10-18", "PG"}, args)
}

func TestOr(t *testing.T) {
	filter := []byte(`[{"$or":[{"ReleaseDate":{"$equals":"2018-10-18"},"Rating":{"$equals":"PG"}}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((ReleaseDate = ? OR Rating = ?))", sql)
	assert.Equal(t, []interface{}{"2018-10-18", "PG"}, args)
}

func TestMapWithMoreThanOneKey(t *testing.T) {
	filter := []byte(`[{"first_name":{"$equals":"Tim"}},{"last_name":{"$equals":"Wolf"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (first_name = ? AND last_name = ?)", sql)
	assert.Equal(t, []interface{}{"Tim", "Wolf"}, args)
}

func TestNestedMapWithMoreThanOneFieldOperator(t *testing.T) {
	filter := []byte(`[{"$or":[{"ReleaseDate":{"$gt":"2018-10-15","$lt":"2018-10-10"}}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((ReleaseDate > ? OR ReleaseDate < ?))", sql)
	assert.Equal(t, []interface{}{"2018-10-15", "2018-10-10"}, args)
}

func TestNestedMapWithMoreThanOneLogicalOperator(t *testing.T) {
	filter := []byte(`[{"$or":[{"first_name":{"$equals":"Tim"}},{"last_name":{"$equals":"Wolf"}},{"home_phone":{"$equals":"919-821-3220"}},{"$and":[{"city":{"$equals":"Chicago"}},{"zip":{"$equals":"12345"}}],"$or":[{"state":{"$equals":"California"}},{"state":{"$equals":"Wisconsin"}},{"$and":[{"postal_code":{"$equals":"21121"}},{"street":{"$equals":"Baker Street"}}]}]}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((first_name = ? OR last_name = ? OR home_phone = ? OR (city = ? AND zip = ?) OR (state = ? OR state = ? OR (postal_code = ? AND street = ?))))", sql)
	assert.Equal(t, []interface{}{"Tim", "Wolf", "919-821-3220", "Chicago", "12345", "California", "Wisconsin", "21121", "Baker Street"}, args)
}
