package mixin

import "gorm.io/gorm"

// IsEnabled 是 GORM 可复用的 mixin，表示是否启用。
// 可嵌入到任意模型：
//
//	type User struct {
//	    mixin.IsEnabled
//	}
//
// 字段使用指针以支持 nullable；`index` 创建索引；BeforeCreate 在未设置时保证为 true。
type IsEnabled struct {
	IsEnabled *bool `gorm:"column:is_enabled;type:boolean;default:true;index" json:"is_enabled,omitempty"`
}

// BeforeCreate 在创建时如果未显式设置，保证默认启用。
func (m *IsEnabled) BeforeCreate(tx *gorm.DB) (err error) {
	if m.IsEnabled == nil {
		v := true
		m.IsEnabled = &v
	}
	return nil
}
