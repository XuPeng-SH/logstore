package sm

import "logstore/pkg/entry"

type tuple struct {
	lsn  uint64
	data []byte
}

func newEmptyTuple() *tuple {
	return &tuple{}
}

func newTuple(e entry.Entry) *tuple {
	row := &tuple{
		lsn:  e.GetInfo().(uint64),
		data: make([]byte, e.GetPayloadSize()),
	}
	copy(row.data, e.GetPayload())
	return row
}

func (t *tuple) Fill(e entry.Entry) {
	t.lsn = e.GetInfo().(uint64)
	t.data = make([]byte, e.GetPayloadSize())
	copy(t.data, e.GetPayload())
}
