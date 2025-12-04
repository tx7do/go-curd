package mixin

import (
	"time"

	"gorm.io/gorm"
)

// CreateTimestamp (create_time)
type CreateTimestamp struct {
	CreateTime *int64 `gorm:"column:create_time;type:bigint" json:"create_time,omitempty"`
}

func (m *CreateTimestamp) BeforeCreate(tx *gorm.DB) (err error) {
	if m.CreateTime == nil {
		now := time.Now().UnixMilli()
		m.CreateTime = &now
	}
	return nil
}

// UpdateTimestamp (update_time)
type UpdateTimestamp struct {
	UpdateTime *int64 `gorm:"column:update_time;type:bigint" json:"update_time,omitempty"`
}

func (m *UpdateTimestamp) BeforeCreate(tx *gorm.DB) (err error) {
	if m.UpdateTime == nil {
		now := time.Now().UnixMilli()
		m.UpdateTime = &now
	}
	return nil
}

func (m *UpdateTimestamp) BeforeSave(tx *gorm.DB) (err error) {
	now := time.Now().UnixMilli()
	m.UpdateTime = &now
	return nil
}

// DeleteTimestamp (delete_time)
type DeleteTimestamp struct {
	DeleteTime *int64 `gorm:"column:delete_time;type:bigint;index" json:"delete_time,omitempty"`
}

func (m *DeleteTimestamp) BeforeDelete(tx *gorm.DB) (err error) {
	if m.DeleteTime == nil {
		now := time.Now().UnixMilli()
		m.DeleteTime = &now
	}
	return nil
}

// Timestamp (create_time + update_time + delete_time)
type Timestamp struct {
	CreateTimestamp
	UpdateTimestamp
	DeleteTimestamp
}

// CreatedAtTimestamp (created_at)
type CreatedAtTimestamp struct {
	CreatedAt *int64 `gorm:"column:created_at;type:bigint" json:"created_at,omitempty"`
}

func (m *CreatedAtTimestamp) BeforeCreate(tx *gorm.DB) (err error) {
	if m.CreatedAt == nil {
		now := time.Now().UnixMilli()
		m.CreatedAt = &now
	}
	return nil
}

// UpdatedAtTimestamp (updated_at)
type UpdatedAtTimestamp struct {
	UpdatedAt *int64 `gorm:"column:updated_at;type:bigint" json:"updated_at,omitempty"`
}

func (m *UpdatedAtTimestamp) BeforeCreate(tx *gorm.DB) (err error) {
	if m.UpdatedAt == nil {
		now := time.Now().UnixMilli()
		m.UpdatedAt = &now
	}
	return nil
}

func (m *UpdatedAtTimestamp) BeforeSave(tx *gorm.DB) (err error) {
	now := time.Now().UnixMilli()
	m.UpdatedAt = &now
	return nil
}

// DeletedAtTimestamp (deleted_at)
type DeletedAtTimestamp struct {
	DeletedAt *int64 `gorm:"column:deleted_at;type:bigint;index" json:"deleted_at,omitempty"`
}

func (m *DeletedAtTimestamp) BeforeDelete(tx *gorm.DB) (err error) {
	if m.DeletedAt == nil {
		now := time.Now().UnixMilli()
		m.DeletedAt = &now
	}
	return nil
}

// TimestampAt (created_at + updated_at + deleted_at)
type TimestampAt struct {
	CreatedAtTimestamp
	UpdatedAtTimestamp
	DeletedAtTimestamp
}
