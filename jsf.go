package jsf

import (
	"encoding/json"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// ApplyFilter transpiles a JSON filter definition onto a SQL query built with the Squirrel SQL builder.
func ApplyFilter(q sq.SelectBuilder, filter []byte) (sq.SelectBuilder, error) {
	var f []interface{}

	if err := json.Unmarshal(filter, &f); err != nil {
		return q, fmt.Errorf("Unrecognizable definition: %v", filter)
	}

	root := sq.And{}
	root, err := applyFilters(root, f)
	if err != nil {
		return q, err
	}

	return q.Where(root), nil
}

// Returns the list of sq.Sqlizers from this segment of the filter definition.
// The segment should contain an array of filter definitions.
func applyFilters(root []sq.Sqlizer, filter []interface{}) ([]sq.Sqlizer, error) {
	for _, v := range filter {
		def, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Unrecognizable definition: %v (expected map[string]interface{}, %T given)", v, v)
		}

		// Produce the sq.Sqlizers for the definitions in filter.
		sqlizers, err := applyFilter(def)
		if err != nil {
			return nil, err
		}

		// Add the children to the root.
		root = append(root, sqlizers...)
	}

	return root, nil
}

// Returns the list of sq.Sqlizers from this segment of the filter definition.
// The segment should contain a map with logical operators or field names as keys.
func applyFilter(filter map[string]interface{}) ([]sq.Sqlizer, error) {
	var conj []sq.Sqlizer

	for op, f := range filter {
		switch op {
		case "$and":
			// Add all filters under the new sub-root sq.And.
			and := sq.And{}
			and, err := applyOperatorFilter(and, f)
			if err != nil {
				return nil, err
			}
			conj = append(conj, and)
		case "$or":
			// Add all filters under the new sub-root sq.Or.
			or := sq.Or{}
			or, err := applyOperatorFilter(or, f)
			if err != nil {
				return nil, err
			}
			conj = append(conj, or)
		default:
			// Add all filters for the specified field.
			sqlizers, err := applyFieldFilter(op, f)
			if err != nil {
				return nil, err
			}
			conj = append(conj, sqlizers...)
		}
	}

	return conj, nil
}

// Returns the list of sq.Sqlizers belonging to the root.
func applyOperatorFilter(root []sq.Sqlizer, filter interface{}) ([]sq.Sqlizer, error) {
	f, ok := filter.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Unrecognizable definition: %v (expected []interface{}, %T given)", filter, filter)
	}

	sqlizers, err := applyFilters(root, f)
	if err != nil {
		return nil, err
	}

	return sqlizers, nil
}

// Returns the list of sq.Sqlizers that filter on the field.
func applyFieldFilter(field string, filter interface{}) ([]sq.Sqlizer, error) {
	var conj []sq.Sqlizer

	f, ok := filter.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Unrecognizable definition: %v (expected map[string]interface{}, %T given)", filter, filter)
	}

	for op, v := range f {
		switch op {
		case "$eq":
			conj = append(conj, sq.Eq{field: v})
		case "$ne":
			conj = append(conj, sq.NotEq{field: v})
		case "$gt":
			conj = append(conj, sq.Gt{field: v})
		case "$gte":
			conj = append(conj, sq.GtOrEq{field: v})
		case "$lt":
			conj = append(conj, sq.Lt{field: v})
		case "$lte":
			conj = append(conj, sq.LtOrEq{field: v})
		case "$isnull":
			conj = append(conj, sq.Eq{field: nil})
		case "$isnotnull":
			conj = append(conj, sq.NotEq{field: nil})
		case "$in":
			conj = append(conj, sq.Eq{field: v})
		case "$notin":
			conj = append(conj, sq.NotEq{field: v})
		default:
			return nil, fmt.Errorf("Unknown operator: %v", op)
		}
	}

	return conj, nil
}
