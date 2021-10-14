package store

import (
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"sync/atomic"
)

type syncBase struct {
	synced, syncing             uint64
	checkpointed, checkpointing uint64
}

func (base *syncBase) OnEntryReceived(e entry.Entry) error {
	if info := e.GetInfo(); info != nil {
		switch v := info.(type) {
		case uint64:
			base.syncing = v
		case *common.ClosedInterval:
			base.checkpointing = v.End
		default:
			panic("not supported")
		}
	}
	return nil
}

func (base *syncBase) GetCheckpointed() uint64 {
	return atomic.LoadUint64(&base.checkpointed)
}

func (base *syncBase) SetCheckpointed(id uint64) {
	atomic.StoreUint64(&base.checkpointed, id)
}

func (base *syncBase) GetSynced() uint64 {
	return atomic.LoadUint64(&base.synced)
}

func (base *syncBase) SetSynced(id uint64) {
	atomic.StoreUint64(&base.synced, id)
}

func (base *syncBase) OnCommit() {
	if base.checkpointing > base.checkpointed {
		base.SetCheckpointed(base.checkpointing)
	}
	if base.syncing > base.synced {
		base.SetSynced(base.syncing)
	}
}
