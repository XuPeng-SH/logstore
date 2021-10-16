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

type tuple struct {
	lsn  uint64
	data []byte
}

func newTuple(e entry.Entry) *tuple {
	row := &tuple{
		lsn:  e.GetInfo().(uint64),
		data: make([]byte, e.GetPayloadSize()),
	}
	copy(row.data, e.GetPayload())
	return row
}

type simpleStateMachine struct {
	mu      *sync.RWMutex
	rows    []*tuple
	visible uint64

	waitingQueue chan entry.Entry
	loopCancel   context.CancelFunc
	loopCtx      context.Context
	loopWg       sync.WaitGroup

	checkpointQueue  chan struct{}
	checkpointCancel context.CancelFunc
	checkpointCtx    context.Context
	checkpointWg     sync.WaitGroup

	wg  sync.WaitGroup
	wal *simpleWal
}

func NewSimpleStateMachine(dir string, walCfg *store.StoreCfg) (*simpleStateMachine, error) {
	wal, err := newSimpleWal(dir, "wal", walCfg)
	if err != nil {
		return nil, err
	}
	sm := &simpleStateMachine{
		mu:              new(sync.RWMutex),
		wal:             wal,
		rows:            make([]*tuple, 0, 100),
		waitingQueue:    make(chan entry.Entry, 1000),
		checkpointQueue: make(chan struct{}, 100),
	}
	sm.loopCtx, sm.loopCancel = context.WithCancel(context.Background())
	sm.checkpointCtx, sm.checkpointCancel = context.WithCancel(context.Background())
	sm.wg.Add(1)
	go sm.checkpointLoop()
	sm.wg.Add(1)
	go sm.waitLoop()
	return sm, nil
}

func (sm *simpleStateMachine) Close() error {
	err := sm.wal.driver.Close()
	if err != nil {
		return err
	}
	sm.loopWg.Wait()
	sm.loopCancel()
	sm.checkpointWg.Wait()
	sm.checkpointCancel()
	sm.wg.Wait()
	return nil
}

func (sm *simpleStateMachine) enqueueWait(e entry.Entry) {
	sm.loopWg.Add(1)
	sm.waitingQueue <- e
}

func (sm *simpleStateMachine) enqueueCheckpoint() {
	sm.checkpointWg.Add(1)
	sm.checkpointQueue <- struct{}{}
}

func (sm *simpleStateMachine) checkpointLoop() {
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

func (sm *simpleStateMachine) waitLoop() {
	defer sm.wg.Done()
	lastCkp := uint64(0)
	for {
		select {
		case <-sm.loopCtx.Done():
			return
		case e := <-sm.waitingQueue:
			err := e.WaitDone()
			if err != nil {
				panic(err)
			}
			visible := e.GetInfo().(uint64)
			atomic.StoreUint64(&sm.visible, visible)
			e.Free()
			if visible >= lastCkp+1000 {
				sm.enqueueCheckpoint()
				lastCkp = visible
				logrus.Infof("checkpoint %d", visible)
			}
			sm.loopWg.Done()
		}
	}
}

func (sm *simpleStateMachine) OnRequest(r *Request) error {
	switch r.Op {
	case TInsert:
		e, err := sm.wal.AsyncLog(r.Op, r.Data)
		if err != nil {
			return err
		}
		return sm.doInsert(e)
	}
	panic("not supported")
}

func (sm *simpleStateMachine) VisibleLSN() uint64 {
	return atomic.LoadUint64(&sm.visible)
}

func (sm *simpleStateMachine) doInsert(e entry.Entry) error {
	row := newTuple(e)
	sm.mu.Lock()
	sm.rows = append(sm.rows, row)
	sm.enqueueWait(e)
	sm.mu.Unlock()
	return nil
}

func (sm *simpleStateMachine) checkpoint() error {
	e := entry.GetBase()
	defer e.Free()
	e.SetType(entry.ETCheckpoint)
	e.SetInfo(&common.ClosedInterval{
		End: sm.VisibleLSN(),
	})
	err := sm.wal.driver.AppendEntry(e)
	if err != nil {
		return err
	}
	if err = e.WaitDone(); err != nil {
		return err
	}
	return sm.wal.driver.TryTruncate()
}
