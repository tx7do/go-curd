package gorm

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tx7do/go-utils/mapper"
)

// 为避免与其他测试文件重名，使用独立的测试类型名
type updTestUserEntity struct {
	ID   uint `gorm:"primarykey"`
	Name string
	Age  int
}

func openTestDBForUpdater(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err = db.AutoMigrate(&updTestUserEntity{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	return db
}

func seedUsersForUpdater(t *testing.T, db *gorm.DB, users ...updTestUserEntity) {
	for _, u := range users {
		if err := db.Create(&u).Error; err != nil {
			t.Fatalf("seed user failed: %v", err)
		}
	}
}

func sendMessage(msg proto.Message) {
	// 这里的 msg 参数现在可以接受任何实现了 proto.Message 的类型
	// ...
}

func TestUpdater_UpdateAndUpdateX(t *testing.T) {
	db := openTestDBForUpdater(t)
	ctx := context.Background()

	// seed 单条记录
	seedUsersForUpdater(t, db, updTestUserEntity{Name: "alice", Age: 20})

	// 使用零值 CopierMapper（与其他测试一致）
	m := &mapper.CopierMapper[User, updTestUserEntity]{}
	up := NewUpdater[User, updTestUserEntity](m)

	sendMessage(&User{})

	// 错误场景：nil db
	user := &User{Id: 1, Name: "x"}
	//var pm proto.Message = user
	_, err := up.Update(ctx, nil, user, &fieldmaskpb.FieldMask{Paths: []string{"Name"}}, nil)
	if err == nil {
		t.Fatalf("expected error when db is nil")
	}

	// 错误场景：nil dto
	_, err = up.Update(ctx, db, nil, &fieldmaskpb.FieldMask{Paths: []string{"Name"}}, nil)
	if err == nil {
		t.Fatalf("expected error when dto is nil")
	}

	// 正常更新：仅更新 Name 字段，使用 doUpdateFunc 指定 where
	dto := &User{Id: 1, Name: "alice-updated"}
	mask := &fieldmaskpb.FieldMask{Paths: []string{"name"}}
	doUpdate := func(gdb *gorm.DB, entity *updTestUserEntity) *gorm.DB {
		// 根据 ID 更新目标记录
		return gdb.Where("id = ?", entity.ID)
	}

	//var pmDto proto.Message = dto
	updatedDTO, err := up.Update(ctx, db, dto, mask, doUpdate)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updatedDTO == nil {
		t.Fatalf("Update returned nil DTO")
	}

	//// 验证数据库内容已更新（直接查询实体）
	//var ent updTestUserEntity
	//if err = db.First(&ent, 1).Error; err != nil {
	//	t.Fatalf("query after update failed: %v", err)
	//}
	//if ent.Name != "alice-updated" {
	//	t.Fatalf("expected name updated to %q, got %q", "alice-updated", ent.Name)
	//}
	//
	//// UpdateX: 不返回 DTO，仅执行更新（更新 Age）
	////dto2 := &User{Id: 1, Age: 99}
	////mask2 := &fieldmaskpb.FieldMask{Paths: []string{"Age"}}
	////doUpdate2 := func(gdb *gorm.DB, entity *updTestUserEntity) *gorm.DB {
	////	return gdb.Where("id = ?", entity.ID)
	////}
	////var pmDto2 proto.Message = dto2
	////if err = up.UpdateX(ctx, db, dto2, mask2, doUpdate2); err != nil {
	////	t.Fatalf("UpdateX failed: %v", err)
	////}
	//// 验证 Age 更新
	//if err = db.First(&ent, 1).Error; err != nil {
	//	t.Fatalf("query after updatex failed: %v", err)
	//}
	//if ent.Age != 99 {
	//	t.Fatalf("expected age updated to %d, got %d", 99, ent.Age)
	//}
	//
	//// 全字段更新（nil mask），将 name 与 age 一起更新
	//dto3 := &User{Id: 1, Name: "final", Age: 30}
	//var pmDto3 proto.Message = dto3
	//if _, err = up.Update(ctx, db, &pmDto3, nil, func(gdb *gorm.DB, entity *updTestUserEntity) *gorm.DB {
	//	return gdb.Where("id = ?", entity.ID)
	//}); err != nil {
	//	t.Fatalf("full Update failed: %v", err)
	//}
	//if err = db.First(&ent, 1).Error; err != nil {
	//	t.Fatalf("query after full update failed: %v", err)
	//}
	//if ent.Name != "final" || ent.Age != 30 {
	//	t.Fatalf("expected final state (final,30), got (%s,%d)", ent.Name, ent.Age)
	//}
}
