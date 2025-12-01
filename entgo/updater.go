package entgo

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/tx7do/go-utils/fieldmaskutil"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-utils/trans"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type UpdateBuilder[ENTITY any] interface {
	Exec(ctx context.Context) error

	ExecX(ctx context.Context)

	Save(ctx context.Context) (*ENTITY, error)

	SaveX(ctx context.Context) *ENTITY
}

type Updater[DTO proto.Message, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]
}

func NewUpdater[DTO proto.Message, ENTITY any](mapper *mapper.CopierMapper[DTO, ENTITY]) *Updater[DTO, ENTITY] {
	return &Updater[DTO, ENTITY]{
		mapper: mapper,
	}
}

// Update 根据实体更新数据
func (up *Updater[DTO, ENTITY]) Update(
	ctx context.Context,
	builder UpdateBuilder[ENTITY],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(builder UpdateBuilder[ENTITY], dto *DTO),
) (*DTO, error) {
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(proto.Message(*dto)), updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return nil, err
	}

	doUpdateFieldFunc(builder, dto)

	var err error
	var entity *ENTITY
	if entity, err = builder.Save(ctx); err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return nil, err
	}

	return up.mapper.ToDTO(entity), nil
}

// UpdateX 仅执行更新操作，不返回更新后的数据
func (up *Updater[DTO, ENTITY]) UpdateX(
	ctx context.Context,
	builder UpdateBuilder[ENTITY],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(builder UpdateBuilder[ENTITY], dto *DTO),
) error {
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(proto.Message(*dto)), updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return err
	}

	doUpdateFieldFunc(builder, dto)

	if err := builder.Exec(ctx); err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return err
	}

	return nil
}
