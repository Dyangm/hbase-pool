package hbase

import (
	"github.com/golang/protobuf/proto"
	"testing"
	"time"
)

func TestNewConsumer(t *testing.T) {
	addr := "192.168.10.51:9090"
	maxConn := 10
	idleTimeout := 60
	pool := NewThriftPool(addr, maxConn, idleTimeout, Dial, Close)

	putEntityHistoryArr := make([]*TPut, 0)
	RowKey := "00000000001"
	Qualifier := "a"
	Timestamp := time.Now().Unix()
	value := "36"
	putEntityHistoryArr = append(putEntityHistoryArr,
		&TPut{
			Row: []byte(RowKey),
			ColumnValues: []*TColumnValue{
				{
					Family:    []byte("f"),
					Qualifier: []byte(Qualifier),
					Timestamp: proto.Int64(int64(Timestamp)),
					Value:     []byte(value),
				},
			}})

	client, err := pool.Get()
	if err != nil {
		t.Fail()
		return
	}

	table := "table"
	err = client.Client.PutMultiple([]byte(table), putEntityHistoryArr)
	if err != nil {
		t.Fail()
		return
	}

	err = pool.Put(client)
	if err != nil {
		t.Fail()
		return
	}

	t.Fail()
}
