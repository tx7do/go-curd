package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Candle struct {
	Symbol    *string                `json:"s"`
	Open      *float64               `json:"o"`
	High      *float64               `json:"h"`
	Low       *float64               `json:"l"`
	Close     *float64               `json:"c"`
	Volume    *float64               `json:"v"`
	StartTime *timestamppb.Timestamp `json:"st"`
	EndTime   *timestamppb.Timestamp `json:"et"`
}

func createTestClient() *Client {
	cli, _ := NewClient(
		WithLogger(log.DefaultLogger),
		WithURI("mongodb://root:123456@127.0.0.1:27017/?compressors=snappy,zlib,zstd"),
		WithDatabase("finances"),
	)
	return cli
}

func TestNewClient(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	client.CheckConnect()
}

func TestInsertOne(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	ctx := context.Background()

	candle := Candle{
		StartTime: timestamppb.New(time.Now()),
		Symbol:    trans.Ptr("AAPL"),
		Open:      trans.Ptr(1.0),
		High:      trans.Ptr(2.0),
		Low:       trans.Ptr(3.0),
		Close:     trans.Ptr(4.0),
		Volume:    trans.Ptr(1000.0),
	}

	_, err := client.InsertOne(ctx, "candles", candle)
	assert.NoError(t, err)
}
