package mixin

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Metadata 存任意 JSON 键值对
type Metadata struct {
	Metadata datatypes.JSONMap `gorm:"column:metadata;type:json" json:"metadata,omitempty"`
}

func (m *Metadata) BeforeSave(tx *gorm.DB) (err error) { return nil }
