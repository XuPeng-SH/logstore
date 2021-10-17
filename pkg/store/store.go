package store

import (
	"context"
	"errors"
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"sync"
)

var (
	DefaultMaxBatchSize = 500
)

type baseStore struct {
	syncBase
	common.Closable
	dir, name   string
	flushWg     sync.WaitGroup
	flushCtx    context.Context
	flushCancel context.CancelFunc
	flushQueue  chan entry.Entry
	wg          sync.WaitGroup
	file        File
	mu          *sync.RWMutex
}

func NewBaseStore(dir, name string, cfg *StoreCfg) (*baseStore, error) {
	var err error
	bs := &baseStore{
		dir:        dir,
		name:       name,
		flushQueue: make(chan entry.Entry, DefaultMaxBatchSize*100),
	}
	bs.file, err = OpenRotateFile(dir, name, nil, cfg.RotateChecker, cfg.HistoryFactory)
	if err != nil {
		return nil, err
	}
	bs.flushCtx, bs.flushCancel = context.WithCancel(context.Background())
	bs.start()
	return bs, nil
}

func (bs *baseStore) start() {
	bs.wg.Add(1)
	go bs.flushLoop()
}

func (bs *baseStore) flushLoop() {
	defer bs.wg.Done()
	entries := make([]entry.Entry, 0, DefaultMaxBatchSize)
	for {
		select {
		case <-bs.flushCtx.Done():
			return
		case e := <-bs.flushQueue:
			entries = append(entries, e)
		Left:
			for i := 0; i < DefaultMaxBatchSize-1; i++ {
				select {
				case e = <-bs.flushQueue:
					entries = append(entries, e)
				default:
					break Left
				}
			}
			cnt := len(entries)
			bs.onEntries(entries)
			entries = entries[:0]
			bs.flushWg.Add(-1 * cnt)
		}
	}
}

func (bs *baseStore) onEntries(entries []entry.Entry) {
	var err error
	// fmt.Printf("entries %d\n", len(entries))
	for _, e := range entries {
		appender := bs.file.GetAppender()
		if err = appender.Prepare(e.TotalSize(), e.GetInfo()); err != nil {
			panic(err)
		}
		if _, err = appender.Write(e.GetMetaBuf()); err != nil {
			panic(err)
		}
		if _, err = appender.Write(e.GetPayload()); err != nil {
			panic(err)
		}
		if err = appender.Commit(); err != nil {
			panic(err)
		}
		bs.OnEntryReceived(e)
	}
	if err = bs.file.Sync(); err != nil {
		panic(err)
	}
	bs.OnCommit()

	for _, e := range entries {
		e.DoneWithErr(nil)
	}
}

func (bs *baseStore) Close() error {
	if !bs.TryClose() {
		return nil
	}
	bs.flushWg.Wait()
	bs.flushCancel()
	bs.wg.Wait()
	return bs.file.Close()
}

func (bs *baseStore) Checkpoint(e entry.Entry) (err error) {
	if e.IsCheckpoint() {
		return errors.New("wrong entry type")
	}
	return bs.AppendEntry(e)
}

func (bs *baseStore) TryTruncate() error {
	return bs.file.GetHistory().TryTruncate()
}

func (bs *baseStore) AppendEntry(e entry.Entry) (err error) {
	if bs.Closed() {
		return common.ClosedErr
	}
	bs.flushWg.Add(1)
	if bs.Closed() {
		bs.flushWg.Done()
		return common.ClosedErr
	}
	bs.flushQueue <- e
	return nil
}
