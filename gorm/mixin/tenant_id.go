package mixin

import "gorm.io/gorm"

// TenantID 是 GORM 可复用的 mixin，表示租户 ID（可为空）。
// 使用指针以支持 nullable，并在数据库中建立索引。
// 不在钩子中强制不可变性（ent 的 Immutable 在 GORM 中需在业务层或更复杂的钩子中处理）。
type TenantID struct {
	TenantID *uint32 `gorm:"column:tenant_id;type:int unsigned;index" json:"tenant_id,omitempty"`
}

func (m *TenantID) BeforeCreate(tx *gorm.DB) (err error) {
	// 保持 nil 或显式设置的值，不做默认填充
	return nil
}

func (m *TenantID) BeforeSave(tx *gorm.DB) (err error) {
	// 不在此处强制不可变性，必要时可在此实现校验
	return nil
}
