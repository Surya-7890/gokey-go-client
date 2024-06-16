package main

import (
	"fmt"
	"net"
)

type Client struct {
	URI   string
	conn  net.Conn
	close chan struct{}
}

func (c *Client) Connect() error {
	conn, err := net.Dial("tcp", c.URI)
	if err != nil {
		return fmt.Errorf("error while connecting to gokey: %s", err.Error())
	}
	c.conn = conn
	c.close = make(chan struct{}, 10)
	return nil
}

func (c *Client) Close() {
	close(c.close)
}

func (c *Client) CreateTable(tablename string) (*Table, error) {

	_, err := c.conn.Write([]byte("CREATE " + tablename))
	if err != nil {
		return &Table{}, fmt.Errorf("error while creating a table: %s", err.Error())
	}

	// the repsonse from the previous action
	buffer := make([]byte, 1024)
	len, err := c.conn.Read(buffer)
	if err != nil {
		return &Table{}, fmt.Errorf("error: %s", err.Error())
	}

	switch string(buffer[:len]) {
	case "success":
		return &Table{
			Name: tablename,
			Conn: c.conn,
		}, nil
	default:
		return &Table{}, fmt.Errorf(string(buffer[:len]))
	}
}
