package mixin

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// UuidID 是 GORM 可复用的 mixin，表示主键 UUID（字符串形式）。
// 使用 char(36) 存储，可在 BeforeCreate/BeforeSave 钩子中确保默认值。
type UuidID struct {
	ID uuid.UUID `gorm:"column:id;type:char(36);primaryKey" json:"id,omitempty"`
}

func (m *UuidID) BeforeCreate(tx *gorm.DB) (err error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	return nil
}

func (m *UuidID) BeforeSave(tx *gorm.DB) (err error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	return nil
}
