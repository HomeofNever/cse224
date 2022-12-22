package test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Client struct {
	Port int

	conn net.Conn
}

func (c *Client) Dial() error {
	var err error
	addr := fmt.Sprintf("localhost:%v", c.Port)
	if c.conn, err = net.Dial("tcp", addr); err != nil {
		return err
	}
	return nil
}

func (c *Client) SendRequestFromFile(path string) error {
	bw := bufio.NewWriter(c.conn)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	br := bufio.NewReader(f)
	if _, err := io.Copy(bw, br); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *Client) ReceiveResponseToFile(path string) error {
	br := bufio.NewReader(c.conn)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	if _, err := io.Copy(bw, br); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
