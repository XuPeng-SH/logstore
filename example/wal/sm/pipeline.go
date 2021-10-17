package sm

import (
	"logstore/pkg/entry"
	"sync"
)

type writePipeline struct {
	mu *sync.RWMutex
	sm *stateMachine
}

func newPipeline(sm *stateMachine) *writePipeline {
	return &writePipeline{
		mu: sm.mu,
		sm: sm,
	}
}

func (p *writePipeline) prepare(r *Request) (*Row, entry.Entry, error) {
	p.mu.Lock()
	row := p.sm.makeRoomForInsert(r.Data)
	e, err := p.sm.wal.PrepareLog(r.Op, r.Data)
	if err != nil {
		panic(err)
	}
	p.mu.Unlock()

	return row, e, err
}

func (p *writePipeline) commit(row *Row, e entry.Entry) error {
	row.Fill(e)
	pending := GetPending(e)
	p.sm.enqueueWait(pending)
	err := pending.WaitDone()
	pending.Free()
	return err
}
