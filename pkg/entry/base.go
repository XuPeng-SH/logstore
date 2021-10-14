package entry

import (
	"io"
	"logstore/pkg/common"
	"sync"
)

var (
	_basePool = sync.Pool{New: func() interface{} {
		return &Base{
			descriptor: newDescriptor(),
		}
	}}
)

type Base struct {
	*descriptor
	node    *common.MemNode
	payload []byte
	info    interface{}
	wg      sync.WaitGroup
	err     error
}

func GetBase() *Base {
	b := _basePool.Get().(*Base)
	b.wg.Add(1)
	return b
}

func (b *Base) reset() {
	b.descriptor.reset()
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	b.payload = nil
	b.info = nil
	b.wg = sync.WaitGroup{}
	b.err = nil
}

func (b *Base) GetError() error {
	return b.err
}

func (b *Base) WaitDone() error {
	b.wg.Wait()
	return b.err
}

func (b *Base) DoneWithErr(err error) {
	b.err = err
	b.wg.Done()
}

func (b *Base) Free() {
	b.reset()
	_basePool.Put(b)
}

func (b *Base) GetPayload() []byte {
	if b.node != nil {
		return b.node.Buf[:b.GetPayloadSize()]
	}
	return b.payload
}

func (b *Base) SetInfo(info interface{}) {
	b.info = info
}

func (b *Base) GetInfo() interface{} {
	return b.info
}

func (b *Base) UnmarshalFromNode(n *common.MemNode, own bool) error {
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	if own {
		b.node = n
		b.payload = b.node.GetBuf()
	} else {
		copy(b.payload, n.GetBuf())
	}
	b.SetPayloadSize(len(b.payload))
	return nil
}

func (b *Base) Unmarshal(buf []byte) error {
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	b.payload = buf
	b.SetPayloadSize(len(buf))
	return nil
}

func (b *Base) ReadFrom(r io.Reader) (int, error) {
	if b.node == nil {
		b.node = common.GPool.Alloc(uint64(b.GetPayloadSize()))
		b.payload = b.node.Buf[:b.GetPayloadSize()]
	}
	return r.Read(b.payload)
}

func (b *Base) WriteTo(w io.Writer) (int, error) {
	n1, err := b.descriptor.WriteTo(w)
	if err != nil {
		return n1, err
	}
	n2, err := w.Write(b.payload)
	if err != nil {
		return n2, err
	}
	return n1 + n2, err
}
