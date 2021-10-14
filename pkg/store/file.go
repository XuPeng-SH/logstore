package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

func MakeVersionFile(dir, name string, version uint64) string {
	return fmt.Sprintf("%s-%d%s", filepath.Join(dir, name), version, ".rot")
}

type idAlloc struct {
	id uint64
}

func (id *idAlloc) Get() uint64 {
	return atomic.LoadUint64(&id.id)
}

func (id *idAlloc) Alloc() uint64 {
	return atomic.AddUint64(&id.id, uint64(1))
}

func (id *idAlloc) Set(val uint64) {
	atomic.StoreUint64(&id.id, val)
}

type files struct {
	files []*vFile
}

type rotateFile struct {
	*sync.RWMutex
	dir, name   string
	checker     RotateChecker
	uncommitted []*vFile
	history     History
	commitMu    sync.RWMutex

	commitWg     sync.WaitGroup
	commitCtx    context.Context
	commitCancel context.CancelFunc
	commitQueue  chan *vFile

	nextVer uint64
	idAlloc idAlloc

	wg sync.WaitGroup
}

func OpenRotateFile(dir, name string, mu *sync.RWMutex, rotateChecker RotateChecker,
	historyFactory HistoryFactory) (*rotateFile, error) {
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}
	if rotateChecker == nil {
		rotateChecker = NewMaxSizeRotateChecker(DefaultRotateCheckerMaxSize)
	}
	if historyFactory == nil {
		historyFactory = DefaultHistoryFactory
	}
	rf := &rotateFile{
		RWMutex:     mu,
		dir:         dir,
		name:        name,
		uncommitted: make([]*vFile, 0),
		checker:     rotateChecker,
		commitQueue: make(chan *vFile, 10000),
		history:     historyFactory(),
	}
	rf.commitCtx, rf.commitCancel = context.WithCancel(context.Background())
	err := rf.scheduleNew()
	rf.wg.Add(1)
	go rf.commitLoop()
	return rf, err
}

func (rf *rotateFile) commitLoop() {
	defer rf.wg.Done()
	for {
		select {
		case <-rf.commitCtx.Done():
			return
		case file := <-rf.commitQueue:
			// fmt.Printf("Receive request: %s\n", file.Name())
			file.Commit()
			rf.commitFile()
			rf.commitWg.Done()
		}
	}
}

func (rf *rotateFile) scheduleCommit(file *vFile) {
	rf.commitWg.Add(1)
	// fmt.Printf("Schedule request: %s\n", file.Name())
	rf.commitQueue <- file
}

func (rf *rotateFile) Close() error {
	rf.commitWg.Wait()
	rf.commitCancel()
	rf.wg.Wait()
	for _, vf := range rf.uncommitted {
		vf.Close()
	}
	return nil
}

func (rf *rotateFile) scheduleNew() error {
	fname := MakeVersionFile(rf.dir, rf.name, rf.nextVer)
	rf.nextVer++
	vf, err := newVFile(nil, fname, int(rf.nextVer))
	if err != nil {
		return err
	}
	rf.uncommitted = append(rf.uncommitted, vf)
	return nil
}

func (rf *rotateFile) getFileState() *vFileState {
	l := len(rf.uncommitted)
	if l == 0 {
		return nil
	}
	return rf.uncommitted[l-1].GetState()
}

func (rf *rotateFile) makeSpace(size int) (rotated *vFile, curr *vFileState, err error) {
	var (
		rotNeeded bool
	)
	l := len(rf.uncommitted)
	if l == 0 {
		rotNeeded, err = rf.checker.PrepareAppend(nil, size)
	} else {
		rotNeeded, err = rf.checker.PrepareAppend(rf.uncommitted[l-1], size)
	}
	if err != nil {
		return nil, nil, err
	}
	if l == 0 || rotNeeded {
		if rotNeeded {
			rotated = rf.uncommitted[l-1]
			rf.scheduleCommit(rotated)
		}
		if err = rf.scheduleNew(); err != nil {
			return nil, nil, err
		}
	}
	curr = rf.getFileState()
	curr.file.PrepareWrite(size)
	return rotated, curr, nil
}

func (rf *rotateFile) GetAppender() FileAppender {
	return newFileAppender(rf)
}

func (rf *rotateFile) commitFile() {
	rf.Lock()
	f := rf.uncommitted[0]
	if !f.HasCommitted() {
		panic("logic error")
	}
	rf.uncommitted = rf.uncommitted[1:]
	rf.history.Append(f)
	rf.Unlock()
	fmt.Printf("Committed %s\n", f.Name())
}

func (rf *rotateFile) Sync() error {
	rf.RLock()
	if len(rf.uncommitted) == 0 {
		rf.RUnlock()
		return nil
	}
	if len(rf.uncommitted) == 1 {
		f := rf.uncommitted[0]
		rf.RUnlock()
		return f.Sync()
	}
	lastFile := rf.uncommitted[len(rf.uncommitted)-1]
	waitFile := rf.uncommitted[len(rf.uncommitted)-2]
	rf.RUnlock()
	waitFile.WaitCommitted()
	return lastFile.Sync()
}
