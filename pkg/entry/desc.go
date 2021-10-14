package entry

import (
	"encoding/binary"
	"io"
	"unsafe"
)

const (
	DescriptorSize = int(unsafe.Sizeof(ETInvalid) + unsafe.Sizeof(uint32(0)))
)

type descriptor struct {
	descBuf []byte
}

func newDescriptor() *descriptor {
	return &descriptor{
		descBuf: make([]byte, DescriptorSize),
	}
}

func (desc *descriptor) IsFlush() bool {
	return desc.GetType() == ETFlush
}

func (desc *descriptor) IsCheckpoint() bool {
	return desc.GetType() == ETCheckpoint
}

func (desc *descriptor) SetType(t Type) {
	binary.BigEndian.PutUint16(desc.descBuf, t)
}

func (desc *descriptor) SetPayloadSize(size int) {
	binary.BigEndian.PutUint32(desc.descBuf[unsafe.Sizeof(ETInvalid):], uint32(size))
}

func (desc *descriptor) reset() {
	desc.SetType(ETInvalid)
	desc.SetPayloadSize(0)
}

func (desc *descriptor) GetMetaBuf() []byte {
	return desc.descBuf
}

func (desc *descriptor) GetType() Type {
	return binary.BigEndian.Uint16(desc.descBuf)
}

func (desc *descriptor) GetPayloadSize() int {
	return int(binary.BigEndian.Uint32(desc.descBuf[unsafe.Sizeof(ETInvalid):]))
}

func (desc *descriptor) TotalSize() int {
	return DescriptorSize + desc.GetPayloadSize()
}

func (desc *descriptor) WriteTo(w io.Writer) (int, error) {
	return w.Write(desc.descBuf)
}

func (desc *descriptor) ReadFrom(r io.Reader) (int, error) {
	return r.Read(desc.descBuf)
}
