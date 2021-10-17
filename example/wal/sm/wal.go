package sm

import (
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"logstore/pkg/store"
	"sync"
)

type OpT = entry.Type

const (
	TCreateTable OpT = iota + entry.ETCustomizedStart
	TDropTable
	TInsert
	TDelete
)

type simpleWal struct {
	mu      sync.Mutex
	driver  store.Store
	idAlloc common.IdAllocator
}

func newSimpleWal(dir, name string, cfg *store.StoreCfg) (*simpleWal, error) {
	dirver, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		return nil, err
	}
	return &simpleWal{driver: dirver}, nil
}

func (wal *simpleWal) AsyncLog(op OpT, item []byte) (entry.Entry, error) {
	var err error
	e := entry.GetBase()
	e.SetType(op)
	e.SetPayloadSize(len(item))
	e.Unmarshal(item)
	wal.mu.Lock()
	id := wal.idAlloc.Alloc()
	e.SetInfo(id)
	err = wal.driver.AppendEntry(e)
	wal.mu.Unlock()
	return e, err
}

func (wal *simpleWal) SyncLog(op OpT, item []byte) error {
	e, err := wal.AsyncLog(op, item)
	if err != nil {
		return err
	}
	err = e.WaitDone()
	item = e.GetPayload()
	e.Free()
	return err
}
