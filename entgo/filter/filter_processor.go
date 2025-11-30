package filter

import (
	"encoding/json"
	"fmt"
	"strings"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/tx7do/go-utils/stringcase"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

// Processor 过滤处理器接口
type Processor struct {
}

// Process 处理过滤条件
func (poc Processor) Process(s *sql.Selector, p *sql.Predicate, op pagination.Operator, field, value string) *sql.Predicate {
	var cond *sql.Predicate

	switch op {
	case pagination.Operator_EQ:
		return poc.filterEqual(s, p, field, value)
	case pagination.Operator_NEQ:
		return poc.filterNot(s, p, field, value)
	case pagination.Operator_IN:
		return poc.filterIn(s, p, field, value)
	case pagination.Operator_NIN:
		return poc.filterNotIn(s, p, field, value)
	case pagination.Operator_GTE:
		return poc.filterGTE(s, p, field, value)
	case pagination.Operator_GT:
		return poc.filterGT(s, p, field, value)
	case pagination.Operator_LTE:
		return poc.filterLTE(s, p, field, value)
	case pagination.Operator_LT:
		return poc.filterLT(s, p, field, value)
	case pagination.Operator_BETWEEN:
		return poc.filterRange(s, p, field, value)
	case pagination.Operator_IS_NULL:
		return poc.filterIsNull(s, p, field, value)
	case pagination.Operator_IS_NOT_NULL:
		return poc.filterIsNotNull(s, p, field, value)
	case pagination.Operator_CONTAINS:
		return poc.filterContains(s, p, field, value)
	case pagination.Operator_ICONTAINS:
		return poc.filterInsensitiveContains(s, p, field, value)
	case pagination.Operator_STARTS_WITH:
		return poc.filterStartsWith(s, p, field, value)
	case pagination.Operator_ISTARTS_WITH:
		return poc.filterInsensitiveStartsWith(s, p, field, value)
	case pagination.Operator_ENDS_WITH:
		return poc.filterEndsWith(s, p, field, value)
	case pagination.Operator_IENDS_WITH:
		return poc.filterInsensitiveEndsWith(s, p, field, value)
	case pagination.Operator_EXACT:
		return poc.filterExact(s, p, field, value)
	case pagination.Operator_IEXACT:
		return poc.filterInsensitiveExact(s, p, field, value)
	case pagination.Operator_REGEXP:
		return poc.filterRegex(s, p, field, value)
	case pagination.Operator_IREGEXP:
		return poc.filterInsensitiveRegex(s, p, field, value)
	case pagination.Operator_SEARCH:
		return poc.filterSearch(s, p, field, value)
	default:
		return nil
	}

	return cond
}

// filterEqual = 相等操作
// SQL: WHERE "name" = "tom"
func (poc Processor) filterEqual(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EQ(s.C(field), value)
}

// filterNot NOT 不相等操作
// SQL: WHERE NOT ("name" = "tom")
// 或者： WHERE "name" <> "tom"
// 用NOT可以过滤出NULL，而用<>、!=则不能。
func (poc Processor) filterNot(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.Not().EQ(s.C(field), value)
}

// filterIn IN操作
// SQL: WHERE name IN ("tom", "jimmy")
func (poc Processor) filterIn(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	var values []any
	if err := json.Unmarshal([]byte(value), &values); err == nil {
		return p.In(s.C(field), values...)
	}
	return nil
}

// filterNotIn NOT IN操作
// SQL: WHERE name NOT IN ("tom", "jimmy")`
func (poc Processor) filterNotIn(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	var values []any
	if err := json.Unmarshal([]byte(value), &values); err == nil {
		return p.NotIn(s.C(field), values...)
	}
	return nil
}

// filterGTE GTE (Greater Than or Equal) 大于等于 >=操作
// SQL: WHERE "create_time" >= "2023-10-25"
func (poc Processor) filterGTE(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.GTE(s.C(field), value)
}

// filterGT GT (Greater than) 大于 >操作
// SQL: WHERE "create_time" > "2023-10-25"
func (poc Processor) filterGT(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.GT(s.C(field), value)
}

// filterLTE LTE (Less Than or Equal) 小于等于 <=操作
// SQL: WHERE "create_time" <= "2023-10-25"
func (poc Processor) filterLTE(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.LTE(s.C(field), value)
}

// filterLT LT (Less than) 小于 <操作
// SQL: WHERE "create_time" < "2023-10-25"
func (poc Processor) filterLT(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.LT(s.C(field), value)
}

// filterRange 在值域之中 BETWEEN操作
// SQL: WHERE "create_time" BETWEEN "2023-10-25" AND "2024-10-25"
// 或者： WHERE "create_time" >= "2023-10-25" AND "create_time" <= "2024-10-25"
func (poc Processor) filterRange(s *sql.Selector, _ *sql.Predicate, field, value string) *sql.Predicate {
	var values []any
	if err := json.Unmarshal([]byte(value), &values); err == nil {
		if len(values) != 2 {
			return nil
		}

		return sql.And(
			sql.GTE(s.C(field), values[0]),
			sql.LTE(s.C(field), values[1]),
		)
	}

	return nil
}

// filterIsNull 为空 IS NULL操作
// SQL: WHERE name IS NULL
func (poc Processor) filterIsNull(s *sql.Selector, p *sql.Predicate, field, _ string) *sql.Predicate {
	return p.IsNull(s.C(field))
}

// filterIsNotNull 不为空 IS NOT NULL操作
// SQL: WHERE name IS NOT NULL
func (poc Processor) filterIsNotNull(s *sql.Selector, p *sql.Predicate, field, _ string) *sql.Predicate {
	return p.Not().IsNull(s.C(field))
}

// filterContains LIKE 前后模糊查询
// SQL: WHERE name LIKE '%L%';
func (poc Processor) filterContains(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.Contains(s.C(field), value)
}

// filterInsensitiveContains ILIKE 前后模糊查询
// SQL: WHERE name ILIKE '%L%';
func (poc Processor) filterInsensitiveContains(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.ContainsFold(s.C(field), value)
}

// filterStartsWith LIKE 前缀+模糊查询
// SQL: WHERE name LIKE 'La%';
func (poc Processor) filterStartsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.HasPrefix(s.C(field), value)
}

// filterInsensitiveStartsWith ILIKE 前缀+模糊查询
// SQL: WHERE name ILIKE 'La%';
func (poc Processor) filterInsensitiveStartsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EqualFold(s.C(field), value+"%")
}

// filterEndsWith LIKE 后缀+模糊查询
// SQL: WHERE name LIKE '%a';
func (poc Processor) filterEndsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.HasSuffix(s.C(field), value)
}

// filterInsensitiveEndsWith ILIKE 后缀+模糊查询
// SQL: WHERE name ILIKE '%a';
func (poc Processor) filterInsensitiveEndsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EqualFold(s.C(field), "%"+value)
}

// filterExact LIKE 操作 精确比对
// SQL: WHERE name LIKE 'a';
func (poc Processor) filterExact(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.Like(s.C(field), value)
}

// filterInsensitiveExact ILIKE 操作 不区分大小写，精确比对
// SQL: WHERE name ILIKE 'a';
func (poc Processor) filterInsensitiveExact(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EqualFold(s.C(field), value)
}

// filterRegex 正则查找
// MySQL: WHERE title REGEXP BINARY '^(An?|The) +'
// Oracle: WHERE REGEXP_LIKE(title, '^(An?|The) +', 'c');
// PostgreSQL: WHERE title ~ '^(An?|The) +';
// SQLite: WHERE title REGEXP '^(An?|The) +';
func (poc Processor) filterRegex(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ~ ")
			b.Arg(value)
			break

		case dialect.MySQL:
			b.Ident(s.C(field)).WriteString(" REGEXP BINARY ")
			b.Arg(value)
			break

		case dialect.SQLite:
			b.Ident(s.C(field)).WriteString(" REGEXP ")
			b.Arg(value)
			break

		case dialect.Gremlin:
			break
		}
	})
	return p
}

// filterInsensitiveRegex 正则查找 不区分大小写
// MySQL: WHERE title REGEXP '^(an?|the) +'
// Oracle: WHERE REGEXP_LIKE(title, '^(an?|the) +', 'i');
// PostgreSQL: WHERE title ~* '^(an?|the) +';
// SQLite: WHERE title REGEXP '(?i)^(an?|the) +';
func (poc Processor) filterInsensitiveRegex(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ~* ")
			b.Arg(strings.ToLower(value))
			break

		case dialect.MySQL:
			b.Ident(s.C(field)).WriteString(" REGEXP ")
			b.Arg(strings.ToLower(value))
			break

		case dialect.SQLite:
			b.Ident(s.C(field)).WriteString(" REGEXP ")
			if !strings.HasPrefix(value, "(?i)") {
				value = "(?i)" + value
			}
			b.Arg(strings.ToLower(value))
			break

		case dialect.Gremlin:
			break
		}
	})
	return p
}

// filterSearch 全文搜索
// SQL:
func (poc Processor) filterSearch(s *sql.Selector, p *sql.Predicate, _, _ string) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {

		}
	})
	return p
}

// filterDatePart 时间戳提取日期
// SQL: select extract(quarter from timestamp '2018-08-15 12:10:10');
func (poc Processor) filterDatePart(s *sql.Selector, p *sql.Predicate, datePart, field string) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			str := fmt.Sprintf("EXTRACT('%s' FROM %s)", strings.ToUpper(datePart), s.C(field))
			b.WriteString(str)
			//b.Arg(strings.ToLower(value))
			break

		case dialect.MySQL:
			str := fmt.Sprintf("%s(%s)", strings.ToUpper(datePart), s.C(field))
			b.WriteString(str)
			//b.Arg(strings.ToLower(value))
			break
		}
	})
	return p
}

// filterDatePartField 日期
func (poc Processor) filterDatePartField(s *sql.Selector, datePart, field string) string {
	p := sql.P()
	switch s.Builder.Dialect() {
	case dialect.Postgres:
		str := fmt.Sprintf("EXTRACT('%s' FROM %s)", strings.ToUpper(datePart), s.C(field))
		p.WriteString(str)
		break

	case dialect.MySQL:
		str := fmt.Sprintf("%s(%s)", strings.ToUpper(datePart), s.C(field))
		p.WriteString(str)
		break
	}

	return p.String()
}

// filterJsonb 提取JSONB字段
// Postgresql: WHERE ("app_profile"."preferences" ->> 'daily_email') = 'true'
func (poc Processor) filterJsonb(s *sql.Selector, p *sql.Predicate, jsonbField, field string) *sql.Predicate {
	field = stringcase.ToSnakeCase(field)

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ->> ").WriteString("'" + jsonbField + "'")
			//b.Arg(strings.ToLower(value))
			break

		case dialect.MySQL:
			str := fmt.Sprintf("JSON_EXTRACT(%s, '$.%s')", s.C(field), jsonbField)
			b.WriteString(str)
			//b.Arg(strings.ToLower(value))
			break
		}
	})
	return p
}

// filterJsonbField JSONB字段
func (poc Processor) filterJsonbField(s *sql.Selector, jsonbField, field string) string {
	field = stringcase.ToSnakeCase(field)

	p := sql.P()
	switch s.Builder.Dialect() {
	case dialect.Postgres:
		p.Ident(s.C(field)).WriteString(" ->> ").WriteString("'" + jsonbField + "'")
		//b.Arg(strings.ToLower(value))
		break

	case dialect.MySQL:
		str := fmt.Sprintf("JSON_EXTRACT(%s, '$.%s')", s.C(field), jsonbField)
		p.WriteString(str)
		//b.Arg(strings.ToLower(value))
		break
	}

	return p.String()
}
