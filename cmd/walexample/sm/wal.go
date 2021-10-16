package sm

import (
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"logstore/pkg/store"
)

type OpT = entry.Type

const (
	TCreateTable OpT = iota + entry.ETCustomizedStart
	TDropTable
	TInsert
	TDelete
)

type simpleWal struct {
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
	id := wal.idAlloc.Alloc()
	e := entry.GetBase()
	e.SetInfo(id)
	e.SetType(op)
	e.SetPayloadSize(len(item))
	e.Unmarshal(item)
	err = wal.driver.AppendEntry(e)
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
