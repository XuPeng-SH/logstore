package common

import (
	"errors"
	"sync/atomic"
)

var (
	ClosedErr = errors.New("closed")
)

type Closable struct {
	closed int32
}

func (c *Closable) Closed() bool {
	return atomic.LoadInt32(&c.closed) == int32(1)
}

func (c *Closable) TryClose() bool {
	if !atomic.CompareAndSwapInt32(&c.closed, int32(0), int32(1)) {
		return false
	}
	return true
}
