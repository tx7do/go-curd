package mixin

import "gorm.io/gorm"

// Status 表示业务状态（例如 0: disabled, 1: enabled）
type Status struct {
	Status uint8 `gorm:"column:status;type:tinyint unsigned;default:1;not null;index" json:"status"`
}

func (m *Status) BeforeCreate(_ *gorm.DB) (err error) {
	if m.Status == 0 {
		m.Status = 1
	}
	return nil
}
