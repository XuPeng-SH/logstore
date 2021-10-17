package sm

import (
	"context"
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"logstore/pkg/store"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type Request struct {
	Op   OpT
	Data []byte
}

type stateMachine struct {
	common.Closable
	mu      *sync.RWMutex
	wal     *Wal
	rows    []*Row
	visible uint64

	waitingQueue chan *pendingEntry
	loopCancel   context.CancelFunc
	loopCtx      context.Context
	loopWg       sync.WaitGroup

	checkpointQueue  chan struct{}
	checkpointCancel context.CancelFunc
	checkpointCtx    context.Context
	checkpointWg     sync.WaitGroup

	pipeline *writePipeline

	wg sync.WaitGroup
}

func NewStateMachine(dir string, walCfg *store.StoreCfg) (*stateMachine, error) {
	wal, err := newWal(dir, "wal", walCfg)
	if err != nil {
		return nil, err
	}
	sm := &stateMachine{
		mu:              new(sync.RWMutex),
		wal:             wal,
		rows:            make([]*Row, 0, 100),
		waitingQueue:    make(chan *pendingEntry, 1000),
		checkpointQueue: make(chan struct{}, 100),
	}
	sm.pipeline = newPipeline(sm)
	sm.loopCtx, sm.loopCancel = context.WithCancel(context.Background())
	sm.checkpointCtx, sm.checkpointCancel = context.WithCancel(context.Background())
	sm.wg.Add(1)
	go sm.checkpointLoop()
	sm.wg.Add(1)
	go sm.waitLoop()
	return sm, nil
}

func (sm *stateMachine) Close() error {
	if !sm.TryClose() {
		return nil
	}
	sm.loopWg.Wait()
	sm.loopCancel()
	sm.checkpointWg.Wait()
	sm.checkpointCancel()
	sm.wg.Wait()
	return sm.wal.Close()
}

func (sm *stateMachine) enqueueWait(e *pendingEntry) error {
	if sm.Closed() {
		return common.ClosedErr
	}
	sm.loopWg.Add(1)
	if sm.Closed() {
		sm.loopWg.Done()
		return common.ClosedErr
	}
	sm.waitingQueue <- e
	return nil
}

func (sm *stateMachine) enqueueCheckpoint() {
	sm.checkpointWg.Add(1)
	sm.checkpointQueue <- struct{}{}
}

func (sm *stateMachine) checkpointLoop() {
	defer sm.wg.Done()
	for {
		select {
		case <-sm.checkpointCtx.Done():
			return
		case <-sm.checkpointQueue:
			sm.checkpoint()
			sm.checkpointWg.Done()
		}
	}
}

func (sm *stateMachine) waitLoop() {
	defer sm.wg.Done()
	lastCkp := uint64(0)
	for {
		select {
		case <-sm.loopCtx.Done():
			return
		case pending := <-sm.waitingQueue:
			err := pending.entry.WaitDone()
			if err != nil {
				panic(err)
			}
			visible := pending.entry.GetInfo().(uint64)
			pending.Done()
			atomic.StoreUint64(&sm.visible, visible)
			if visible >= lastCkp+1000 {
				sm.enqueueCheckpoint()
				lastCkp = visible
				logrus.Infof("checkpoint %d", visible)
			}
			sm.loopWg.Done()
		}
	}
}

func (sm *stateMachine) OnRequest(r *Request) error {
	switch r.Op {
	case TInsert:
		return sm.onInsert(r)
	}
	panic("not supported")
}

func (sm *stateMachine) onInsert(r *Request) error {
	row, e, err := sm.pipeline.prepare(r)
	if err != nil {
		return err
	}
	return sm.pipeline.commit(row, e)
}

func (sm *stateMachine) VisibleLSN() uint64 {
	return atomic.LoadUint64(&sm.visible)
}

func (sm *stateMachine) makeRoomForInsert(buf []byte) *Row {
	row := newRow()
	sm.rows = append(sm.rows, row)
	return row
}

func (sm *stateMachine) checkpoint() error {
	e := entry.GetBase()
	defer e.Free()
	e.SetType(entry.ETCheckpoint)
	e.SetInfo(&common.ClosedInterval{
		End: sm.VisibleLSN(),
	})
	err := sm.wal.AppendEntry(e)
	if err != nil {
		return err
	}
	if err = e.WaitDone(); err != nil {
		return err
	}
	return sm.wal.TryTruncate()
}
