//go:build windows

package nntp

import "syscall"

func (c *Client) socketControl() func(network, address string, rc syscall.RawConn) error {
	rb, wb := c.sockReadBuf, c.sockWriteBuf
	if rb <= 0 && wb <= 0 {
		return nil
	}
	return func(_, _ string, rc syscall.RawConn) error {
		return rc.Control(func(fd uintptr) {
			handle := syscall.Handle(fd)
			if rb > 0 {
				_ = syscall.SetsockoptInt(handle, syscall.SOL_SOCKET, syscall.SO_RCVBUF, rb)
			}
			if wb > 0 {
				_ = syscall.SetsockoptInt(handle, syscall.SOL_SOCKET, syscall.SO_SNDBUF, wb)
			}
		})
	}
}
