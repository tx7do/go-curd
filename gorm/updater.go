package gorm

import (
	"context"

	"gorm.io/gorm"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/tx7do/go-utils/fieldmaskutil"
	"github.com/tx7do/go-utils/mapper"
)

type Updater[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]
}

func NewUpdater[DTO any, ENTITY any](mapper *mapper.CopierMapper[DTO, ENTITY]) *Updater[DTO, ENTITY] {
	return &Updater[DTO, ENTITY]{
		mapper: mapper,
	}
}

func (up *Updater[DTO, ENTITY]) FilterByFieldMask(msg proto.Message, fm *fieldmaskpb.FieldMask) error {
	if msg == nil || fm == nil {
		return nil
	}

	// 规范化并验证字段掩码
	fm.Normalize()

	if err := fieldmaskutil.ValidateFieldMask(msg, fm); err != nil {
		return err
	}

	// 应用字段掩码过滤
	fieldmaskutil.NestedMaskFromPaths(fm.GetPaths()).Filter(msg)
	return nil
}

// Update 根据 DTO 与 updateMask 更新并返回更新后的 DTO。
// doUpdateFunc 可选，用于在执行保存前调整 *gorm.DB（例如添加 Where/Select/Clauses 等）。
func (up *Updater[DTO, ENTITY]) Update(
	ctx context.Context,
	db *gorm.DB,
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFunc func(db *gorm.DB, entity *ENTITY) *gorm.DB,
) (*DTO, error) {
	if db == nil {
		return nil, ErrNilDB()
	}

	var anyDto any = dto
	if anyDto == nil {
		return nil, ErrNilDTO()
	}

	pm, _ := anyDto.(proto.Message)
	if err := up.FilterByFieldMask(pm, updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return nil, err
	}

	entity := up.mapper.ToEntity(dto)
	if entity == nil {
		return nil, ErrMapToEntity()
	}

	gdb := db.WithContext(ctx).Model(entity)
	if doUpdateFunc != nil {
		gdb = doUpdateFunc(gdb, entity)
	}

	if err := gdb.Save(entity).Error; err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return nil, err
	}

	respDto := up.mapper.ToDTO(entity)
	var anyResp any = respDto
	if anyResp == nil {
		return nil, ErrMapToEntity()
	}
	return respDto, nil
}

// 错误构造函数

func ErrNilDB() error {
	return gorm.ErrInvalidDB
}

func ErrNilDTO() error {
	return gorm.ErrInvalidData
}

func ErrMapToEntity() error {
	return gorm.ErrInvalidData
}
