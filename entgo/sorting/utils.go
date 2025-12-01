package sorting

import "entgo.io/ent/dialect/sql"

// buildOrderBySelector 构建字段选择器
func buildOrderBySelector(s *sql.Selector, field string, desc bool) {
	if desc {
		s.OrderBy(sql.Desc(s.C(field)))
	} else {
		s.OrderBy(sql.Asc(s.C(field)))
	}
}
