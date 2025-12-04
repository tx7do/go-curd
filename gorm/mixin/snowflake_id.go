package mixin

import (
	"gorm.io/gorm"

	"github.com/tx7do/go-utils/id"
)

// SnowflakeID 是 GORM 可复用的 mixin，用于使用 Sonyflake 生成的 uint64 主键。
// 嵌入到模型后会在创建前自动填充 ID（如果为 0）。
type SnowflakeID struct {
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement:false;type:bigint" json:"id,omitempty"`
}

// BeforeCreate 在创建记录前如果 ID 为 0 则使用 Sonyflake 填充。
func (m *SnowflakeID) BeforeCreate(tx *gorm.DB) (err error) {
	if m.ID == 0 {
		m.ID = id.GenerateSonyflakeID()
	}
	return nil
}
