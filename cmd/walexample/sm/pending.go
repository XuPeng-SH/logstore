package sm

import (
	"logstore/pkg/entry"
	"sync"
)

var (
	_pool = sync.Pool{New: func() interface{} {
		return &pendingEntry{}
	}}
)

type pendingEntry struct {
	wg    sync.WaitGroup
	entry entry.Entry
}

func GetPending(e entry.Entry) *pendingEntry {
	pending := _pool.Get().(*pendingEntry)
	pending.wg.Add(1)
	pending.entry = e
	return pending
}

func (w *pendingEntry) reset() {
	w.wg = sync.WaitGroup{}
	w.entry.Free()
	w.entry = nil
}

func (w *pendingEntry) Free() {
	w.reset()
	_pool.Put(w)
}

func (w *pendingEntry) WaitDone() error {
	w.wg.Wait()
	return w.entry.GetError()
}

func (w *pendingEntry) Done() {
	w.wg.Done()
}
