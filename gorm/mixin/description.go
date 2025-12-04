package mixin

// Description 是 GORM 可复用的 mixin，支持 nullable 描述字段。
type Description struct {
	Description *string `gorm:"column:description;type:text" json:"description,omitempty"`
}
