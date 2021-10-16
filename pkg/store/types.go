package store

import (
	"io"
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"sync"
)

type StoreCfg struct {
	RotateChecker  RotateChecker
	HistoryFactory HistoryFactory
}

type RotateChecker interface {
	PrepareAppend(VFile, int) (bool, error)
}

type VFile interface {
	sync.Locker
	RLock()
	RUnlock()
	SizeLocked() int
	Destroy() error
	Id() int
	Name() string
	String() string
	InCheckpoint(*common.ClosedInterval) bool
	InCommits(*common.ClosedInterval) bool
	MergeCheckpoint(*common.ClosedInterval) *common.ClosedInterval
}

type FileAppender interface {
	Prepare(int, interface{}) error
	Write([]byte) (int, error)
	Commit() error
	Rollback()
	Sync() error
	Revert()
}

type FileReader interface {
	// io.Reader
	// ReadAt([]byte, FileAppender) (int, error)
}

type ReplayObserver interface {
	OnNewEntry(int)
	OnNewCommit(uint64)
	OnNewCheckpoint(common.ClosedInterval)
}

type ReplayHandle = func(VFile, ReplayObserver) error

type History interface {
	String() string
	Append(VFile)
	Extend(...VFile)
	Entries() int
	EntryIds() []int
	GetEntry(int) VFile
	DropEntry(int) (VFile, error)
	OldestEntry() VFile
	Empty() bool
	Replay(ReplayHandle, ReplayObserver) error
	TryTruncate() error
}

type File interface {
	io.Closer
	sync.Locker
	RLock()
	RUnlock()
	FileReader

	Sync() error
	GetAppender() FileAppender

	GetHistory() History
}

type Store interface {
	io.Closer
	AppendEntry(entry.Entry) error
	TryTruncate() error
}
