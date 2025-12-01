package sorting

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
)

func TestQueryStringSorting_BuildSelector_Empty(t *testing.T) {
	qss := NewQueryStringSorting()

	selFunc, err := qss.BuildSelector(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc != nil {
		t.Fatal("expected nil selector for empty orderBys")
	}
}

func TestQueryStringSorting_BuildSelector_Orderings(t *testing.T) {
	qss := NewQueryStringSorting()

	orderBys := []string{"name", "-age", "", "-", "created_at"}
	selFunc, err := qss.BuildSelector(orderBys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc == nil {
		t.Fatal("expected non-nil selector function")
	}

	// 构造基础 selector 并应用
	s := sql.Dialect(dialect.MySQL).Select("t.*").From(sql.Table("t"))
	selFunc(s)
	sqlStr, _ := s.Query()

	if !strings.Contains(strings.ToUpper(sqlStr), "ORDER BY") {
		t.Fatalf("expected ORDER BY in SQL, got: %s", sqlStr)
	}
	// 检查包含字段名和方向（尽量宽松匹配）
	if !strings.Contains(sqlStr, "name") {
		t.Fatalf("expected ordering by name, got: %s", sqlStr)
	}
	if !strings.Contains(strings.ToUpper(sqlStr), "AGE") || !strings.Contains(strings.ToUpper(sqlStr), "DESC") {
		t.Fatalf("expected ordering by age DESC, got: %s", sqlStr)
	}
	if !strings.Contains(sqlStr, "created_at") {
		t.Fatalf("expected ordering by created_at, got: %s", sqlStr)
	}
}

func TestQueryStringSorting_BuildSelectorWithDefaultField(t *testing.T) {
	qss := NewQueryStringSorting()

	// 当 orderBys 为空时，应使用默认字段和方向
	selFunc, err := qss.BuildSelectorWithDefaultField(nil, "created_at", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc == nil {
		t.Fatal("expected non-nil selector function when default field provided")
	}
	s := sql.Select().From(sql.Table("t"))
	selFunc(s)
	sqlStr, _ := s.Query()
	if !strings.Contains(strings.ToUpper(sqlStr), "ORDER BY") || !strings.Contains(strings.ToUpper(sqlStr), "CREATED_AT") || !strings.Contains(strings.ToUpper(sqlStr), "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got: %s", sqlStr)
	}

	// 当提供 orderBys 时，应优先使用 orderBys 而不是默认字段
	selFunc2, err := qss.BuildSelectorWithDefaultField([]string{"-score"}, "created_at", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc2 == nil {
		t.Fatal("expected non-nil selector function when orderBys provided")
	}
	s2 := sql.Select().From(sql.Table("t"))
	selFunc2(s2)
	sqlStr2, _ := s2.Query()
	if strings.Contains(strings.ToUpper(sqlStr2), "CREATED_AT") {
		t.Fatalf("did not expect default field to be used when orderBys is provided, got: %s", sqlStr2)
	}
	if !strings.Contains(strings.ToUpper(sqlStr2), "SCORE") || !strings.Contains(strings.ToUpper(sqlStr2), "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got: %s", sqlStr2)
	}
}
