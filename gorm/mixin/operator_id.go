package mixin

import "gorm.io/gorm"

// CreateBy 表示创建操作的操作者 ID（只允许在 Create 时写入）。
type CreateBy struct {
	CreateBy *uint32 `gorm:"column:create_by;index;<-:create" json:"create_by,omitempty"`
}

// UpdateBy 表示更新操作的操作者 ID。
type UpdateBy struct {
	UpdateBy *uint32 `gorm:"column:update_by;index" json:"update_by,omitempty"`
}

// DeleteBy 表示删除操作的操作者 ID。
type DeleteBy struct {
	DeleteBy *uint32 `gorm:"column:delete_by;index" json:"delete_by,omitempty"`
}

// CreatedBy 与 CreateBy 等价，按需使用不同列名。
type CreatedBy struct {
	CreatedBy *uint32 `gorm:"column:created_by;index;<-:create" json:"created_by,omitempty"`
}

// UpdatedBy 与 UpdateBy 等价，按需使用不同列名。
type UpdatedBy struct {
	UpdatedBy *uint32 `gorm:"column:updated_by;index" json:"updated_by,omitempty"`
}

// DeletedBy 与 DeleteBy 等价，按需使用不同列名。
type DeletedBy struct {
	DeletedBy *uint32 `gorm:"column:deleted_by;index" json:"deleted_by,omitempty"`
}

// OperatorID 组合已创建/已更新/已删除的操作者 ID 字段，方便在模型中嵌入复用。
type OperatorID struct {
	CreatedBy
	UpdatedBy
	DeletedBy
}

// 可选：如果需要在创建/更新时自动填充操作者 ID，
// 在具体模型或全局回调中实现相应逻辑，示例钩子放在模型层实现。
var _ = gorm.DeletedAt{} // 占位引用，防止未使用 import 警告（如无需要可删除）
