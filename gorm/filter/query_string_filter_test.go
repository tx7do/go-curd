package filter

import (
	"strings"
	"testing"
)

func TestQueryStringFilter_BuildSelectorsAndSimpleEQ(t *testing.T) {
	db := openTestDB(t)
	sf := NewQueryStringFilter()

	andJson := `{"name":"tom","title__contains":"Go"}`
	selectors, err := sf.BuildSelectors(andJson, "")
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if len(selectors) != 1 {
		t.Fatalf("expected 1 selector, got %d", len(selectors))
	}
	sql := sqlFor(t, db, selectors[0])
	lsql := strings.ToLower(sql)
	if sql == "" || !strings.Contains(lsql, "name") || !strings.Contains(lsql, "=") {
		t.Fatalf("unexpected sql for EQ: %q", sql)
	}
	if !strings.Contains(lsql, "like") || !strings.Contains(lsql, "title") {
		t.Fatalf("unexpected sql for CONTAINS: %q", sql)
	}
}

func TestQueryStringFilter_InBetweenAndJsonb(t *testing.T) {
	db := openTestDB(t)
	sf := NewQueryStringFilter()

	// IN 操作（JSON 数组字符串）
	inJson := `{"name__in":"[\"a\",\"b\"]"}`
	sel, err := sf.QueryCommandToWhereConditions(inJson, false)
	if err != nil {
		t.Fatalf("QueryCommandToWhereConditions error: %v", err)
	}
	sql := sqlFor(t, db, sel)
	lsql := strings.ToLower(sql)
	if sql == "" || !strings.Contains(lsql, " in ") {
		t.Fatalf("unexpected sql for IN: %q", sql)
	}

	// BETWEEN / Range 操作
	betweenJson := `{"created_at__between":"[\"2020-01-01\",\"2021-01-01\"]"}`
	sel2, err := sf.QueryCommandToWhereConditions(betweenJson, false)
	if err != nil {
		t.Fatalf("QueryCommandToWhereConditions error: %v", err)
	}
	sql2 := sqlFor(t, db, sel2)
	if sql2 == "" || !(strings.Contains(sql2, ">=") && strings.Contains(sql2, "<=")) {
		t.Fatalf("unexpected sql for BETWEEN: %q", sql2)
	}

	// JSONB 字段 equality (preferences.daily_email == "true")
	jsonb := `{"preferences.daily_email":"true"}`
	sel3, err := sf.QueryCommandToWhereConditions(jsonb, false)
	if err != nil {
		t.Fatalf("QueryCommandToWhereConditions error: %v", err)
	}
	sql3 := sqlFor(t, db, sel3)
	lsql3 := strings.ToLower(sql3)
	if sql3 == "" || !strings.Contains(lsql3, "preferences") || !strings.Contains(lsql3, "daily_email") {
		t.Fatalf("unexpected sql for JSONB: %q", sql3)
	}
}

func TestQueryStringFilter_DatePartAndOperators(t *testing.T) {
	db := openTestDB(t)
	sf := NewQueryStringFilter()

	// Date part 比较，例如 created_at__year__gt
	dateJson := `{"created_at__year__gt":"2020"}`
	sel, err := sf.QueryCommandToWhereConditions(dateJson, false)
	if err != nil {
		t.Fatalf("QueryCommandToWhereConditions error: %v", err)
	}
	sql := sqlFor(t, db, sel)
	if sql == "" {
		t.Fatalf("DatePart produced empty sql")
	}
	lsql := strings.ToLower(sql)
	if !(strings.Contains(lsql, "extract") || strings.Contains(lsql, "date") || strings.Contains(lsql, "year") || strings.Contains(lsql, ">")) {
		t.Fatalf("unexpected sql for DatePart: %q", sql)
	}

	// exact / neq
	eqJson := `{"status":"active","name__neq":"bob"}`
	sel2, err := sf.QueryCommandToWhereConditions(eqJson, false)
	if err != nil {
		t.Fatalf("QueryCommandToWhereConditions error: %v", err)
	}
	sql2 := sqlFor(t, db, sel2)
	lsql2 := strings.ToLower(sql2)
	if !strings.Contains(lsql2, "status") || !strings.Contains(lsql2, "=") {
		t.Fatalf("unexpected sql for EQ in combined: %q", sql2)
	}
	if !strings.Contains(lsql2, "name") || !(strings.Contains(lsql2, " not ") || strings.Contains(lsql2, "!=")) {
		// allow different dialect forms for NOT/NEQ
		t.Fatalf("unexpected sql for NEQ in combined: %q", sql2)
	}
}

func TestQueryStringFilter_EmptyAndInvalid(t *testing.T) {
	sf := NewQueryStringFilter()

	// 空字符串返回 nil selector
	sel, err := sf.QueryCommandToWhereConditions("", false)
	if err != nil {
		t.Fatalf("expected nil error for empty, got %v", err)
	}
	if sel != nil {
		t.Fatalf("expected nil selector for empty input")
	}

	// 无效 JSON 返回错误
	_, err = sf.QueryCommandToWhereConditions("not a json", false)
	if err == nil {
		t.Fatalf("expected error for invalid json")
	}
}
