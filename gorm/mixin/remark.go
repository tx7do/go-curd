package mixin

// Remark 是 GORM 可复用的 mixin，支持 nullable 的备注字段。
type Remark struct {
	Remark *string `gorm:"column:remark;type:text" json:"remark,omitempty"`
}
