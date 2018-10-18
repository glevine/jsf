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

	fm, ok := f.(map[string]interface{})
	if !ok {
		return q, errors.New("Invalid filter")
	}

	root := sq.And{}
	s, err := applyFilters(root, fm)
	if err != nil {
		return q, err
	}
	root = s

	return q.Where(root), nil
}

func applyFilters(s []sq.Sqlizer, fm map[string]interface{}) ([]sq.Sqlizer, error) {
	for op, value := range fm {
		ns, err := applyFilter(op, value)
		if err != nil {
			return s, err
		}

		s = append(s, ns)
	}

	return s, nil
}

func applyFilter(op string, f interface{}) (sq.Sqlizer, error) {
	switch op {
	case "$and":
		fm, ok := f.([]interface{})
		if !ok {
			return nil, errors.New("$and must be an array")
		}

		and := sq.And{}

		for _, v := range fm {
			vm, ok := v.(map[string]interface{})
			if !ok {
				return nil, errors.New("$and must be an array of objects")
			}

			a, err := applyFilters(and, vm)
			if err != nil {
				return nil, err
			}
			and = a
		}

		return and, nil
	case "$or":
		fm, ok := f.([]interface{})
		if !ok {
			return nil, errors.New("$or must be an array")
		}

		or := sq.Or{}

		for _, v := range fm {
			vm, ok := v.(map[string]interface{})
			if !ok {
				return nil, errors.New("$or must be an array of objects")
			}

			o, err := applyFilters(or, vm)
			if err != nil {
				return nil, err
			}
			or = o
		}

		return or, nil
	default:
		return applyFieldFilter(op, f)
	}
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
