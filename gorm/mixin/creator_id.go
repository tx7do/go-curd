package mixin

// CreatorID 是 GORM 风格的 mixin，可被模型通过嵌入复用。
// - 使用指针类型支持 null/nil。
// - `gorm:"<-:create"` 只允许在 Create 时写入（GORM 层面不可在 Update 时修改）。
// - `index` 为该列创建索引。
type CreatorID struct {
	CreatorID *uint32 `gorm:"column:creator_id;index;<-:create" json:"creator_id,omitempty"`
}
