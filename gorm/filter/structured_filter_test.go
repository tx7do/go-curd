package filter

import (
	"strings"
	"testing"

	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/encoding/protojson"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func mustMarshal(fe *pagination.FilterExpr) string {
	b, _ := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}.Marshal(fe)
	return string(b)
}

func TestFilterExprExamples_Marshal(t *testing.T) {
	fe := &pagination.FilterExpr{
		Type: pagination.ExprType_AND,
		Conditions: []*pagination.Condition{
			{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")},
			{Field: "B", Op: pagination.Operator_EQ, Value: trans.Ptr("2")},
		},
	}
	js := mustMarshal(fe)
	if js == "" {
		t.Fatal("protojson marshal returned empty string")
	}
}

func TestNewStructuredFilter_Basics(t *testing.T) {
	sf := NewStructuredFilter()
	if sf == nil {
		t.Fatal("NewStructuredFilter returned nil")
	}
	if sf.processor == nil {
		t.Fatal("expected processor to be initialized")
	}
}

func TestBuildFilterSelectors_NilAndUnspecified(t *testing.T) {
	sf := NewStructuredFilter()

	// nil expr -> empty slice
	sels, err := sf.BuildSelectors(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if sels == nil {
		t.Log("BuildSelectors(nil) returned nil (acceptable)")
	} else if len(sels) != 0 {
		t.Fatalf("expected 0 selectors for nil expr, got %d", len(sels))
	}

	// unspecified -> nil, nil per implementation
	expr := &pagination.FilterExpr{Type: pagination.ExprType_EXPR_TYPE_UNSPECIFIED}
	sels2, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	if sels2 != nil {
		t.Fatalf("expected nil selectors for unspecified expr, got %v", sels2)
	}
}

func TestBuildFilterSelectors_SimpleAnd(t *testing.T) {
	sf := NewStructuredFilter()
	expr := &pagination.FilterExpr{
		Type: pagination.ExprType_AND,
		Conditions: []*pagination.Condition{
			{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")},
		},
	}
	sels, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sels) != 1 {
		t.Fatalf("expected 1 selector for simple AND expr, got %d", len(sels))
	}
	if sels[0] == nil {
		t.Fatal("expected non-nil selector function")
	}
}

func Test_buildFilterSelector_NilAndUnspecified(t *testing.T) {
	sf := NewStructuredFilter()

	sel, err := sf.buildFilterSelector(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if sel != nil {
		t.Fatal("expected nil selector for nil expr, got non-nil")
	}

	expr := &pagination.FilterExpr{Type: pagination.ExprType_EXPR_TYPE_UNSPECIFIED}
	sel2, err := sf.buildFilterSelector(expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	if sel2 != nil {
		t.Fatal("expected nil selector for unspecified expr, got non-nil")
	}
}

func TestStructuredFilter_SupportedOperators_CreateSelectors(t *testing.T) {
	sf := NewStructuredFilter()

	// 仅测试实现中明确支持的操作集合
	ops := []struct {
		name   string
		op     pagination.Operator
		value  string
		values []string
	}{
		{"EQ", pagination.Operator_EQ, "v1", nil},
		{"NEQ", pagination.Operator_NEQ, "v1", nil},
		{"GT", pagination.Operator_GT, "10", nil},
		{"GTE", pagination.Operator_GTE, "10", nil},
		{"LT", pagination.Operator_LT, "10", nil},
		{"LTE", pagination.Operator_LTE, "10", nil},
		{"IN", pagination.Operator_IN, `["a","b"]`, nil},
		{"NIN", pagination.Operator_NIN, `["a","b"]`, nil},
		{"BETWEEN", pagination.Operator_BETWEEN, `["1","5"]`, nil},
		{"IS_NULL", pagination.Operator_IS_NULL, "", nil},
		{"IS_NOT_NULL", pagination.Operator_IS_NOT_NULL, "", nil},
		{"CONTAINS", pagination.Operator_CONTAINS, "sub", nil},
		{"ICONTAINS", pagination.Operator_ICONTAINS, "sub", nil},
		{"STARTS_WITH", pagination.Operator_STARTS_WITH, "pre", nil},
		{"ISTARTS_WITH", pagination.Operator_ISTARTS_WITH, "pre", nil},
		{"ENDS_WITH", pagination.Operator_ENDS_WITH, "suf", nil},
		{"IENDS_WITH", pagination.Operator_IENDS_WITH, "suf", nil},
		{"EXACT", pagination.Operator_EXACT, "exact", nil},
		{"IEXACT", pagination.Operator_IEXACT, "iexact", nil},
		{"REGEXP", pagination.Operator_REGEXP, `^a`, nil},
		{"IREGEXP", pagination.Operator_IREGEXP, `(?i)^a`, nil},
		{"SEARCH", pagination.Operator_SEARCH, "q", nil},
	}

	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			cond := &pagination.Condition{
				Field:  "test_field",
				Op:     tc.op,
				Value:  trans.Ptr(tc.value),
				Values: tc.values,
			}
			expr := &pagination.FilterExpr{
				Type:       pagination.ExprType_AND,
				Conditions: []*pagination.Condition{cond},
			}
			sels, err := sf.BuildSelectors(expr)
			if err != nil {
				t.Fatalf("operator %s: unexpected error: %v", tc.name, err)
			}
			if sels == nil {
				t.Fatalf("operator %s: expected selectors slice, got nil", tc.name)
			}
			if len(sels) != 1 {
				t.Fatalf("operator %s: expected 1 selector, got %d", tc.name, len(sels))
			}
			if sels[0] == nil {
				t.Fatalf("operator %s: expected non-nil selector function", tc.name)
			}
		})
	}
}

func TestStructuredFilter_JSONField_SQL(t *testing.T) {
	sf := NewStructuredFilter()
	db := openTestDB(t)

	// JSON 字段条件： preferences.daily_email = "true"
	cond := &pagination.Condition{
		Field: "preferences.daily_email",
		Op:    pagination.Operator_EQ,
		Value: trans.Ptr("true"),
	}
	expr := &pagination.FilterExpr{
		Type:       pagination.ExprType_AND,
		Conditions: []*pagination.Condition{cond},
	}

	sels, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if len(sels) != 1 {
		t.Fatalf("expected 1 selector, got %d", len(sels))
	}
	sql := sqlFor(t, db, sels[0])
	lsql := strings.ToLower(sql)
	if sql == "" {
		t.Fatalf("expected non-empty sql for jsonb condition")
	}
	if !strings.Contains(lsql, "preferences") {
		t.Fatalf("expected sql to reference preferences, got: %q", sql)
	}
	// json key may appear as literal or as JSON_EXTRACT etc., check key presence
	if !strings.Contains(lsql, "daily_email") && !strings.Contains(lsql, "->>") && !strings.Contains(lsql, "json_extract") {
		t.Fatalf("expected json key or json extract operator in sql, got: %q", sql)
	}
}
