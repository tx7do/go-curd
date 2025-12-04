package mixin

// SoftDelete 组合 DeletedAt 与 DeletedBy，供实体直接嵌入使用。
type SoftDelete struct {
	DeletedAt
	DeletedBy
}
