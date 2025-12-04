package mixin

import "gorm.io/gorm"

// Tag 是 GORM 可复用的 mixin，表示对象关联的标签。
// 使用指针切片以支持 nullable，并通过 gorm 的 json 序列化器存储。
type Tag struct {
	Tags *[]string `gorm:"column:tags;type:json;serializer:json" json:"tags,omitempty"`
}

func (m *Tag) BeforeCreate(tx *gorm.DB) (err error) {
	if m.Tags == nil {
		var v []string
		m.Tags = &v
	}
	return nil
}

func (m *Tag) BeforeSave(tx *gorm.DB) (err error) {
	if m.Tags == nil {
		var v []string
		m.Tags = &v
	}
	return nil
}
