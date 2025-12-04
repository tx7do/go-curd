package mixin

import (
	"fmt"

	"gorm.io/gorm"
)

// 支持的状态值
const (
	SwitchStatusOff = "OFF"
	SwitchStatusOn  = "ON"
)

// SwitchStatus 是 GORM 可复用的 mixin，表示开关状态（可空）。
// 字段使用指针以支持 nullable，带有 gorm 标签与 json 标签。
// BeforeCreate/BeforeSave 钩子用于填充默认值并校验合法枚举。
type SwitchStatus struct {
	Status *string `gorm:"column:status;type:varchar(10);default:ON;index" json:"status,omitempty"`
}

func (m *SwitchStatus) BeforeCreate(tx *gorm.DB) (err error) {
	if m.Status == nil {
		v := SwitchStatusOn
		m.Status = &v
	}
	return m.validate()
}

func (m *SwitchStatus) BeforeSave(tx *gorm.DB) (err error) {
	return m.validate()
}

func (m *SwitchStatus) validate() error {
	if m.Status == nil {
		// 允许为 nil（nullable）
		return nil
	}
	if *m.Status != SwitchStatusOn && *m.Status != SwitchStatusOff {
		return fmt.Errorf("invalid switch status: %s", *m.Status)
	}
	return nil
}
