package jsf

import (
	"encoding/json"
	"fmt"
	"sort"

	sq "github.com/Masterminds/squirrel"
)

// ApplyFilter transpiles a JSON filter definition onto a SQL query built with the Squirrel SQL builder.
func ApplyFilter(q sq.SelectBuilder, filter []byte) (sq.SelectBuilder, error) {
	if len(filter) == 0 {
		return q, nil
	}

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

	operators := alphabeticallyOrderedOperators(filter)
	for _, op := range operators {
		if f, ok := filter[op]; ok {
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

	operators := alphabeticallyOrderedOperators(f)
	for _, op := range operators {
		if v, ok := f[op]; ok {
			switch op {
			case "$equals":
				conj = append(conj, sq.Eq{field: v})
			case "$gt":
				conj = append(conj, sq.Gt{field: v})
			case "$gte":
				conj = append(conj, sq.GtOrEq{field: v})
			case "$in":
				conj = append(conj, sq.Eq{field: v})
			case "$is_null":
				conj = append(conj, sq.Eq{field: nil})
			case "$lt":
				conj = append(conj, sq.Lt{field: v})
			case "$lte":
				conj = append(conj, sq.LtOrEq{field: v})
			case "$not_equals":
				conj = append(conj, sq.NotEq{field: v})
			case "$not_in":
				conj = append(conj, sq.NotEq{field: v})
			case "$not_null":
				conj = append(conj, sq.NotEq{field: nil})
			default:
				return nil, fmt.Errorf("Unknown operator: %v", op)
			}
		}
	}

	return conj, nil
}

// The order of iteration over a map is unpredictable.
// A predictable order is necessary to write reliable tests that can predict the SQL output.
// We control the insertion order of filters into slices by iterating over a slice of operators.
// The order of iteration over a slice is always the insertion order.
// We can guarantee that the SQL will follow this pre-determined order.
func alphabeticallyOrderedOperators(filter map[string]interface{}) []string {
	operators := make([]string, len(filter))
	i := 0

	for op := range filter {
		operators[i] = op
		i++
	}
	sort.Strings(operators)

	return operators
}
