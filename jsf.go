package jsf

import (
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

func ApplyFilter(q sq.SelectBuilder, filter []byte) (sq.SelectBuilder, error) {
	var f interface{}

	if err := json.Unmarshal(filter, &f); err != nil {
		return q, errors.New("Failed to unmarshal the filter")
	}

	fa, ok := f.([]interface{})
	if !ok {
		return q, errors.New("Invalid filter")
	}

	root := sq.And{}
	s, err := applyFilters(root, fa)
	if err != nil {
		return q, err
	}
	root = s

	return q.Where(root), nil
}

func applyFilters(s []sq.Sqlizer, fa []interface{}) ([]sq.Sqlizer, error) {
	for _, value := range fa {
		vm, ok := value.(map[string]interface{})
		if !ok {
			return nil, errors.New("Must be a map")
		}

		ns, err := applyFilter(vm)
		if err != nil {
			return nil, err
		}

		s = append(s, ns...)
	}

	return s, nil
}

func applyFilter(f map[string]interface{}) ([]sq.Sqlizer, error) {
	var conj []sq.Sqlizer

	for op, value := range f {
		switch op {
		case "$and":
			fa, ok := value.([]interface{})
			if !ok {
				return nil, errors.New("$and must be an array")
			}

			and := sq.And{}
			a, err := applyFilters(and, fa)
			if err != nil {
				return nil, err
			}
			and = a
			conj = append(conj, and)
		case "$or":
			fa, ok := value.([]interface{})
			if !ok {
				return nil, errors.New("$or must be an array")
			}

			or := sq.Or{}
			o, err := applyFilters(or, fa)
			if err != nil {
				return nil, err
			}
			or = o
			conj = append(conj, or)
		default:
			a, err := applyFieldFilter(op, value)
			if err != nil {
				return nil, err
			}
			conj = append(conj, a)
		}
	}

	return conj, nil
}

func applyFieldFilter(field string, f interface{}) (sq.Sqlizer, error) {
	var and sq.And

	fm, ok := f.(map[string]interface{})
	if !ok {
		return nil, errors.New("Could not parse the filter")
	}

	for op, v := range fm {
		switch op {
		case "$eq":
			and = append(and, sq.Eq{field: v})
		case "$ne":
			and = append(and, sq.NotEq{field: v})
		case "$gt":
			and = append(and, sq.Gt{field: v})
		case "$gte":
			and = append(and, sq.GtOrEq{field: v})
		case "$lt":
			and = append(and, sq.Lt{field: v})
		case "$lte":
			and = append(and, sq.LtOrEq{field: v})
		case "$isnull":
			and = append(and, sq.Eq{field: nil})
		case "$isnotnull":
			and = append(and, sq.NotEq{field: nil})
		case "$in":
			and = append(and, sq.Eq{field: v})
		case "$notin":
			and = append(and, sq.NotEq{field: v})
		default:
			return and, fmt.Errorf("Invalid operand: %v", op)
		}
	}

	return and, nil
}
