package filter

import (
	"strings"

	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-curd/paginator"

	"github.com/go-kratos/kratos/v2/encoding"

	"github.com/tx7do/go-utils/stringcase"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSONB字段分隔符
)

// StringlyFilter 字符串过滤器
type StringlyFilter struct {
}

func NewStringlyFilter() *StringlyFilter {
	return &StringlyFilter{}
}

// splitQueryKey 分割查询键
func splitQueryKey(key string) []string {
	return strings.Split(key, QueryDelimiter)
}

// splitJsonFieldKey 分割JSON字段键
func splitJsonFieldKey(key string) []string {
	return strings.Split(key, JsonFieldDelimiter)
}

// isJsonFieldKey 是否为JSON字段键
func isJsonFieldKey(key string) bool {
	return strings.Contains(key, JsonFieldDelimiter)
}

// hasOperations 是否有操作
func hasOperations(str string) bool {
	str = strings.ToLower(str)
	paginator.ConverterStringToOperator(str)
	for _, item := range ops {
		if str == item {
			return true
		}
	}
	return false
}

// hasDatePart 是否有日期部分
func hasDatePart(str string) bool {
	str = strings.ToLower(str)
	for _, item := range dateParts {
		if str == item {
			return true
		}
	}
	return false
}

// BuildFilterSelector 构建过滤选择器
func BuildFilterSelector(andFilterJsonString, orFilterJsonString string) (error, []func(s *sql.Selector)) {
	var err error
	var queryConditions []func(s *sql.Selector)

	var andSelector func(s *sql.Selector)
	err, andSelector = QueryCommandToWhereConditions(andFilterJsonString, false)
	if err != nil {
		return err, nil
	}
	if andSelector != nil {
		queryConditions = append(queryConditions, andSelector)
	}

	var orSelector func(s *sql.Selector)
	err, orSelector = QueryCommandToWhereConditions(orFilterJsonString, true)
	if err != nil {
		return err, nil
	}
	if orSelector != nil {
		queryConditions = append(queryConditions, orSelector)
	}

	return nil, queryConditions
}

// QueryCommandToWhereConditions 查询命令转换为选择条件
func QueryCommandToWhereConditions(strJson string, isOr bool) (error, func(s *sql.Selector)) {
	if len(strJson) == 0 {
		return nil, nil
	}

	codec := encoding.GetCodec("json")

	queryMap := make(map[string]string)
	var queryMapArray []map[string]string
	if err1 := codec.Unmarshal([]byte(strJson), &queryMap); err1 != nil {
		if err2 := codec.Unmarshal([]byte(strJson), &queryMapArray); err2 != nil {
			return err2, nil
		}
	}

	return nil, func(s *sql.Selector) {
		var ps []*sql.Predicate
		ps = append(ps, processQueryMap(s, queryMap)...)
		for _, v := range queryMapArray {
			ps = append(ps, processQueryMap(s, v)...)
		}

		if isOr {
			s.Where(sql.Or(ps...))
		} else {
			s.Where(sql.And(ps...))
		}
	}
}

// processQueryMap 处理查询映射表
func processQueryMap(s *sql.Selector, queryMap map[string]string) []*sql.Predicate {
	var ps []*sql.Predicate
	for k, v := range queryMap {
		keys := splitQueryKey(k)

		if cond := makeFieldFilter(s, keys, v); cond != nil {
			ps = append(ps, cond)
		}
	}

	return ps
}

// makeFieldFilter 构建一个字段过滤器
func makeFieldFilter(s *sql.Selector, keys []string, value string) *sql.Predicate {
	if len(keys) == 0 {
		return nil
	}
	if len(value) == 0 {
		return nil
	}

	field := keys[0]
	if len(field) == 0 {
		return nil
	}

	p := sql.P()

	switch len(keys) {
	case 1:
		if isJsonFieldKey(field) {
			jsonFields := splitJsonFieldKey(field)
			if len(jsonFields) != 2 {
				field = stringcase.ToSnakeCase(field)
				return filterEqual(s, p, field, value)
			}
			//value = "'" + value + "'"
			return filterJsonb(
				s, p,
				stringcase.ToSnakeCase(jsonFields[1]),
				stringcase.ToSnakeCase(jsonFields[0]),
			).
				EQ("", value)
		}
		field = stringcase.ToSnakeCase(field)
		return filterEqual(s, p, field, value)

	case 2:
		op := keys[1]
		if len(op) == 0 {
			return nil
		}

		if isJsonFieldKey(field) {
			jsonFields := splitJsonFieldKey(field)
			if len(jsonFields) == 2 {
				field = filterJsonbField(s,
					stringcase.ToSnakeCase(jsonFields[1]),
					stringcase.ToSnakeCase(jsonFields[0]),
				)
				//value = "'" + value + "'"
			}
		} else {
			field = stringcase.ToSnakeCase(field)
		}

		var cond *sql.Predicate
		if hasOperations(op) {
			return processOp(s, p, op, field, value)
		} else if hasDatePart(op) {
			cond = filterDatePart(s, p, op, field).EQ("", value)
		} else {
			cond = filterJsonb(s, p, op, field).EQ("", value)
		}

		return cond

	case 3:
		op1 := keys[1]
		if len(op1) == 0 {
			return nil
		}

		op2 := keys[2]
		if len(op2) == 0 {
			return nil
		}

		// 第二个参数，要么是提取日期，要么是json字段。

		//var cond *sql.Predicate
		if hasDatePart(op1) {
			if isJsonFieldKey(field) {
				jsonFields := splitJsonFieldKey(field)
				if len(jsonFields) == 2 {
					field = filterJsonbField(s, jsonFields[1], jsonFields[0])
					//value = "'" + value + "'"
				}
			} else {
				field = stringcase.ToSnakeCase(field)
			}

			str := filterDatePartField(s, op1, field)

			if hasOperations(op2) {
				return processOp(s, p, op2, str, value)
			}

			return nil
		} else {
			str := filterJsonbField(s, op1, field)

			if hasOperations(op2) {
				return processOp(s, p, op2, str, value)
			} else if hasDatePart(op2) {
				return filterDatePart(s, p, op2, str)
			}
			return nil
		}

	default:
		return nil
	}
}
