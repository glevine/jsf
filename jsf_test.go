package jsf_test

import (
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/glevine/jsf"
	"github.com/stretchr/testify/assert"
)

func ExampleApplyFilter() {
	filter := []byte(`[{"$or":[{"MovieName":{"$equals":"Godzilla"}},{"Rating":{"$equals":"R"}},{"ReleaseDate":{"$gt":"2000-01-01"}},{"$and":[{"PlotSummary":{"$not_null":true}},{"LeadActor":{"$equals":"Harrison Ford"}},{"$or":[{"LeadActor":{"$equals":"Tom Cruise"}},{"ReleaseDate":{"$lte":"2000-01-01"}},{"$and":[{"MovieName":{"$equals":"A Few Good Men"}},{"LeadActress":{"$equals":"Demi Moore"}}]}]}]}]}]`)
	q := sq.Select("MovieName", "Rating").From("db")

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
	// SELECT MovieName, Rating FROM db WHERE ((MovieName = ? OR Rating = ? OR ReleaseDate > ? OR (PlotSummary IS NOT NULL AND LeadActor = ? AND (LeadActor = ? OR ReleaseDate <= ? OR (MovieName = ? AND LeadActress = ?)))))
	// [Godzilla R 2000-01-01 Harrison Ford Tom Cruise 2000-01-01 A Few Good Men Demi Moore]
}

func TestNoFilter(t *testing.T) {
	filter := []byte("")
	q := sq.Select("*").From("db")
	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)
	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db", sql)
	assert.Empty(t, args)
}

func TestEquals(t *testing.T) {
	filter := []byte(`[{"MovieName":{"$equals":"Godzilla"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (MovieName = ?)", sql)
	assert.Equal(t, []interface{}{"Godzilla"}, args)
}

func TestNotEquals(t *testing.T) {
	filter := []byte(`[{"MovieName":{"$not_equals":"Godzilla"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((MovieName <> ? OR MovieName IS NULL))", sql)
	assert.Equal(t, []interface{}{"Godzilla"}, args)
}

func TestGreaterThan(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$gt":"2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate > ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestGreaterThanOrEqualTo(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$gte":"2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate >= ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestLessThan(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$lt":"2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate < ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestLessThanOrEqualTo(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$lte":"2018-10-18"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate <= ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18"}, args)
}

func TestIsNull(t *testing.T) {
	filter := []byte(`[{"PlotSummary":{"$is_null":true}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, _, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (PlotSummary IS NULL)", sql)
}

func TestIsNotNull(t *testing.T) {
	filter := []byte(`[{"PlotSummary":{"$not_null":true}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, _, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (PlotSummary IS NOT NULL)", sql)
}

func TestIn(t *testing.T) {
	filter := []byte(`[{"MovieName":{"$in":["Godzilla","King Kong vs. Godzilla"]}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (MovieName IN (?,?))", sql)
	assert.Equal(t, []interface{}{"Godzilla", "King Kong vs. Godzilla"}, args)
}

func TestNotIn(t *testing.T) {
	filter := []byte(`[{"MovieName":{"$not_in":["Godzilla","King Kong vs. Godzilla"]}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((MovieName NOT IN (?,?) OR MovieName IS NULL))", sql)
	assert.Equal(t, []interface{}{"Godzilla", "King Kong vs. Godzilla"}, args)
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
	filter := []byte(`[{"$or":[{"Rating":{"$equals":"PG-13"}},{"Rating":{"$equals":"PG"}}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((Rating = ? OR Rating = ?))", sql)
	assert.Equal(t, []interface{}{"PG-13", "PG"}, args)
}

func TestMapWithMoreThanOneKey(t *testing.T) {
	filter := []byte(`[{"ReleaseDate":{"$equals":"2018-10-18"}},{"Rating":{"$equals":"PG"}}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE (ReleaseDate = ? AND Rating = ?)", sql)
	assert.Equal(t, []interface{}{"2018-10-18", "PG"}, args)
}

func TestNestedMapWithMoreThanOneFieldOperator(t *testing.T) {
	filter := []byte(`[{"$or":[{"ReleaseDate":{"$gt":"2018-10-10","$lt":"2018-10-18"}}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((ReleaseDate > ? OR ReleaseDate < ?))", sql)
	assert.Equal(t, []interface{}{"2018-10-10", "2018-10-18"}, args)
}

func TestNestedMapWithMoreThanOneLogicalOperator(t *testing.T) {
	filter := []byte(`[{"$or":[{"MovieName":{"$equals":"Godzilla"}},{"Rating":{"$equals":"R"}},{"ReleaseDate":{"$gt":"2000-01-01"}},{"$and":[{"PlotSummary":{"$not_null":true}},{"LeadActor":{"$equals":"Harrison Ford"}}],"$or":[{"LeadActor":{"$equals":"Tom Cruise"}},{"ReleaseDate":{"$lte":"2000-01-01"}},{"$and":[{"MovieName":{"$equals":"A Few Good Men"}},{"LeadActress":{"$equals":"Demi Moore"}}]}]}]}]`)
	q := sq.Select("*").From("db")

	q, err := jsf.ApplyFilter(q, filter)
	assert.NoError(t, err)

	sql, args, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM db WHERE ((MovieName = ? OR Rating = ? OR ReleaseDate > ? OR (PlotSummary IS NOT NULL AND LeadActor = ?) OR (LeadActor = ? OR ReleaseDate <= ? OR (MovieName = ? AND LeadActress = ?))))", sql)
	assert.Equal(t, []interface{}{"Godzilla", "R", "2000-01-01", "Harrison Ford", "Tom Cruise", "2000-01-01", "A Few Good Men", "Demi Moore"}, args)
}

func TestUnknownOperator(t *testing.T) {
	for _, lo := range []string{"$and", "$or"} {
		filter := fmt.Sprintf(`[{"%s":[{"ReleaseDate":{"$equals":"2018-10-18"}},{"Rating":{"$foo":"PG"}}]}]`, lo)
		q := sq.Select("*").From("db")

		q, err := jsf.ApplyFilter(q, []byte(filter))
		assert.Error(t, err)

		sql, args, err := q.ToSql()
		assert.NoError(t, err)
		assert.Equal(t, "SELECT * FROM db", sql)
		assert.Empty(t, args)
	}
}

func TestUnrecognizableDefinition(t *testing.T) {
	for _, filter := range map[string]string{
		"Not JSON":                            "value",
		"Must be surrounded by an array":      `{"$and":[{"field":{"$eq":"value"}}]}`,
		"Array elements must be objects":      `["value"]`,
		"Array elements can't be more arrays": `[["value"]]`,
		"$and/$or keys must have arrays":      `[{"$and":{"$or":[{"field":{"$eq":"value"}}]}}]`,
		"field key must have an object":       `[{"field":"value"}]`,
	} {
		q := sq.Select("*").From("db")

		q, err := jsf.ApplyFilter(q, []byte(filter))
		assert.Error(t, err)

		sql, args, err := q.ToSql()
		assert.NoError(t, err)
		assert.Equal(t, "SELECT * FROM db", sql)
		assert.Empty(t, args)
	}
}
