package store

import (
	"logstore/pkg/common"
	"os"
	"sync"
	"sync/atomic"
)

type vFileState struct {
	pos  int
	file *vFile
}

type rangeInfo struct {
	data        common.ClosedInterval
	checkpoints []common.ClosedInterval
}

type vFile struct {
	*sync.RWMutex
	*os.File
	version    int
	committed  int32
	info       rangeInfo
	parentMu   *sync.RWMutex
	size       int
	wg         sync.WaitGroup
	commitCond sync.Cond
}

func newVFile(mu *sync.RWMutex, name string, version int) (*vFile, error) {
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	return &vFile{
		RWMutex:    mu,
		File:       file,
		version:    version,
		commitCond: *sync.NewCond(new(sync.Mutex)),
	}, nil
}

func (vf *vFile) Id() int {
	return vf.version
}

func (vf *vFile) GetState() *vFileState {
	vf.RLock()
	defer vf.RUnlock()
	return &vFileState{
		pos:  vf.size,
		file: vf,
	}
}

func (vf *vFile) HasCommitted() bool {
	return atomic.LoadInt32(&vf.committed) == int32(1)
}

func (vf *vFile) PrepareWrite(size int) {
	// fmt.Printf("PrepareWrite %s\n", vf.Name())
	vf.wg.Add(1)
	vf.size += size
}

func (vf *vFile) FinishWrite() {
	// fmt.Printf("FinishWrite %s\n", vf.Name())
	vf.wg.Done()
}

func (vf *vFile) Commit() {
	// fmt.Printf("Committing %s\n", vf.Name())
	vf.wg.Wait()
	vf.Sync()
	vf.commitCond.L.Lock()
	atomic.StoreInt32(&vf.committed, int32(1))
	vf.commitCond.Broadcast()
	vf.commitCond.L.Unlock()
}

func (vf *vFile) WaitCommitted() {
	if atomic.LoadInt32(&vf.committed) == int32(1) {
		return
	}
	vf.commitCond.L.Lock()
	if atomic.LoadInt32(&vf.committed) != int32(1) {
		vf.commitCond.Wait()
	}
	vf.commitCond.L.Unlock()
}

func (vf *vFile) WriteAt(b []byte, off int64) (n int, err error) {
	n, err = vf.File.WriteAt(b, off)
	if err != nil {
		return
	}
	return
}

func (vf *vFile) Write(b []byte) (n int, err error) {
	n, err = vf.File.Write(b)
	if err != nil {
		return
	}
	return
}

func (vf *vFile) SizeLocked() int {
	return vf.size
}

func (vf *vFile) Destroy() error {
	if err := vf.Close(); err != nil {
		return err
	}
	name := vf.Name()
	// logutil.Infof("Removing version file: %s", name)
	err := os.Remove(name)
	return err

}
