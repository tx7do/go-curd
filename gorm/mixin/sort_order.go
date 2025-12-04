package mixin

import "gorm.io/gorm"

// SortOrder 是 GORM 可复用的 mixin，表示排序序号（值越小越靠前）。
// 字段使用指针以支持 nullable，带有 gorm 标签和 json 标签。
// BeforeCreate 在创建时如果未显式设置，保证默认为 0。
type SortOrder struct {
	SortOrder *int32 `gorm:"column:sort_order;type:int;default:0;index" json:"sort_order,omitempty"`
}

func (m *SortOrder) BeforeCreate(tx *gorm.DB) (err error) {
	if m.SortOrder == nil {
		v := int32(0)
		m.SortOrder = &v
	}
	return nil
}
