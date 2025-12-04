package mixin

import (
	"time"

	"gorm.io/gorm"
)

// CreatedAt created_at
type CreatedAt struct {
	CreatedAt *time.Time `gorm:"column:created_at;type:datetime" json:"created_at,omitempty"`
}

func (m *CreatedAt) BeforeCreate(tx *gorm.DB) (err error) {
	if m.CreatedAt == nil {
		now := time.Now()
		m.CreatedAt = &now
	}
	return nil
}

// UpdatedAt updated_at
type UpdatedAt struct {
	UpdatedAt *time.Time `gorm:"column:updated_at;type:datetime" json:"updated_at,omitempty"`
}

func (m *UpdatedAt) BeforeCreate(tx *gorm.DB) (err error) {
	if m.UpdatedAt == nil {
		now := time.Now()
		m.UpdatedAt = &now
	}
	return nil
}

func (m *UpdatedAt) BeforeSave(tx *gorm.DB) (err error) {
	now := time.Now()
	m.UpdatedAt = &now
	return nil
}

// DeletedAt deleted_at
type DeletedAt struct {
	DeletedAt *time.Time `gorm:"column:deleted_at;type:datetime;index" json:"deleted_at,omitempty"`
}

func (m *DeletedAt) BeforeDelete(tx *gorm.DB) (err error) {
	if m.DeletedAt == nil {
		now := time.Now()
		m.DeletedAt = &now
	}
	return nil
}

// TimeAt (CreatedAt + UpdatedAt + DeletedAt)
type TimeAt struct {
	CreatedAt
	UpdatedAt
	DeletedAt
}

// CreateTime (create_time)
type CreateTime struct {
	CreateTime *time.Time `gorm:"column:create_time;type:datetime" json:"create_time,omitempty"`
}

func (m *CreateTime) BeforeCreate(tx *gorm.DB) (err error) {
	if m.CreateTime == nil {
		now := time.Now()
		m.CreateTime = &now
	}
	return nil
}

// UpdateTime (update_time)
type UpdateTime struct {
	UpdateTime *time.Time `gorm:"column:update_time;type:datetime" json:"update_time,omitempty"`
}

func (m *UpdateTime) BeforeCreate(tx *gorm.DB) (err error) {
	if m.UpdateTime == nil {
		now := time.Now()
		m.UpdateTime = &now
	}
	return nil
}

func (m *UpdateTime) BeforeSave(tx *gorm.DB) (err error) {
	now := time.Now()
	m.UpdateTime = &now
	return nil
}

// DeleteTime (delete_time)
type DeleteTime struct {
	DeleteTime *time.Time `gorm:"column:delete_time;type:datetime;index" json:"delete_time,omitempty"`
}

func (m *DeleteTime) BeforeDelete(tx *gorm.DB) (err error) {
	if m.DeleteTime == nil {
		now := time.Now()
		m.DeleteTime = &now
	}
	return nil
}

// Time (CreateTime + UpdateTime + DeleteTime)
type Time struct {
	CreateTime
	UpdateTime
	DeleteTime
}
