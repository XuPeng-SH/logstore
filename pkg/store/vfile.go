package store

import (
	"bytes"
	"fmt"
	"logstore/pkg/common"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type vFileState struct {
	pos  int
	file *vFile
}

type vFile struct {
	*sync.RWMutex
	*os.File
	vInfo
	version    int
	committed  int32
	size       int
	wg         sync.WaitGroup
	commitCond sync.Cond
	history    History
}

func newVFile(mu *sync.RWMutex, name string, version int, history History) (*vFile, error) {
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
		history:    history,
	}, nil
}

func (vf *vFile) InCommits(o *common.ClosedInterval) bool {
	return o.Contains(vf.commits)
}

// TODO: process multi checkpoints.
func (vf *vFile) InCheckpoint(o *common.ClosedInterval) bool {
	if len(vf.checkpoints) == 0 {
		return true
	}
	return o.Contains(vf.checkpoints[0])
}

func (vf *vFile) MergeCheckpoint(o *common.ClosedInterval) *common.ClosedInterval {
	if len(vf.checkpoints) == 0 {
		return o
	}
	if o == nil {
		ret := vf.checkpoints[0]
		return &ret
	}
	o.TryMerge(vf.checkpoints[0])
	return o
}

func (vf *vFile) String() string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("[%s]%s", vf.Name(), vf.vInfo.String()))
	return w.String()
}

func (vf *vFile) Archive() error {
	if vf.history == nil {
		if err := vf.Destroy(); err != nil {
			return err
		}
	}
	vf.history.Append(vf)
	return nil
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
	log.Infof("Removing version file: %s", name)
	err := os.Remove(name)
	return err

}
