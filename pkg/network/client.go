package network

import (
	"time"
	"net"
	"context"

	"ddb/pkg/partition"
)

// TODO : client struct for the NodeClient interface 
type Client struct {
	dialTimeout time.Duration
	readTimeout time.Duration
}

// TODO : function to create a new tcp client
func NewClient(dialTimeout, readTimeout, time.Duration) *Client {
	return &Client {
		dialTimeout : dialTimeout,
		readTimeout : readTimeout,
	}
}

// TODO : Now we will send PUT request to a remote node

func(c *Client) Put(ctx context.Context, node *partition.Node, key, value string) error {
	conn, err := c.dial(ctx, node.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", node.Address, err)
	}

	defer conn.Close()

	req := &Request{Op:"PUT", Key : key, Value : value}
	if err := c.sendRequest(ctx, conn, req); err != nil {
		return "", fmt.Errof("failed to send put req %w", err)
	}

	resp, err := c.readRequest(ctx, conn)
	if err!=nil {
		return fmt.Errorf("failed to read PUT response : %w", err)
	}

	if resp.Status != "OK" {
		return fmt.Errorf("PUT failed : %s", resp.Error)
	}
	return nil
}

// TODO : main components of the client will be send request and read request

func (c *Client) sendRequest(ctx context.Context, conn net.Conn, req *Reqest) error{
	data, err := MarshalRequest(req)
	if err := nil {
		return err
	}

	data = append(data, "\n")
	_, err = conn.Write(data)
	return err
}

// TODO : writing the readResponse logic

func (c *Client) readRequest(ctx context.Context, conn net.Conn, req *Request) error {
	// array of byte of 4096 bytes
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	// returning the response from a json req
	return UnMarshalRequest(buf[:n])
}
