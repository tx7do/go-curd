package mixin

import (
	"fmt"
	"regexp"

	"github.com/tx7do/go-utils/id"
	"gorm.io/gorm"
)

var stringIDRegexp = regexp.MustCompile(`^[0-9a-zA-Z_\-]+$`)

// StringID 是 GORM 可复用的 mixin，提供长度上限为 25 的字符串主键字段。
// 嵌入到模型后会在创建前自动生成 ID（当为空时），并校验格式与长度。
type StringID struct {
	ID string `gorm:"column:id;type:varchar(25);primaryKey" json:"id,omitempty"`
}

func (m *StringID) BeforeCreate(_ *gorm.DB) (err error) {
	if m.ID == "" {
		m.ID = id.NewXID()
	}
	if len(m.ID) > 25 {
		return fmt.Errorf("id too long: max 25 characters")
	}
	if !stringIDRegexp.MatchString(m.ID) {
		return fmt.Errorf("invalid id format")
	}
	return nil
}
