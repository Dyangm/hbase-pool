package hbase

import (
	"git.apache.org/thrift.git/lib/go/thrift"
)

func Dial(addr string) (*IdleClient, error) {
	i := &IdleClient{}

	var err error
	i.Socket, err = thrift.NewTSocket(addr)
	if err != nil {
		return nil, err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	i.Client = NewTHBaseServiceClientFactory(i.Socket, protocolFactory)

	err = i.Client.Transport.Open()
	if err != nil {
		return nil, err
	}

	return i, nil
}

func (c *IdleClient) Close() error {
	err := c.Socket.Close()
	//err = c.Send.(*tutorial.PlusServiceClient).Transport.Close()
	return err
}

func Close(c *IdleClient) error {
	err := c.Socket.Close()
	//err = c.Send.(*tutorial.PlusServiceClient).Transport.Close()
	return err
}
