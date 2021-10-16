package sm

import "logstore/pkg/entry"

type writePipeline struct {
	sm *simpleStateMachine
}

func (p *writePipeline) prepare(r *Request) (*tuple, entry.Entry, error) {
	p.sm.mu.Lock()
	row := p.sm.prepareInsert()
	e, err := p.sm.wal.AsyncLog(r.Op, r.Data)
	if err != nil {
		panic(err)
	}
	p.sm.mu.Unlock()

	return row, e, err
}

func (p *writePipeline) commit(row *tuple, e entry.Entry) error {
	row.Fill(e)
	pending := GetPending(e)
	p.sm.enqueueWait(pending)
	err := pending.WaitDone()
	pending.Free()
	return err
}
