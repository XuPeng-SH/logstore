package store

import (
	"bytes"
	"context"
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 20),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.WaitDone()
				assert.Nil(t, err)
				t.Logf("synced %d", s.GetSynced())
				// t.Logf("checkpointed %d", s.GetCheckpointed())
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 1000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()
	cnt := 5
	for i := 0; i < cnt; i++ {
		e := entry.GetBase()
		e.SetType(entry.ETFlush)
		if i%2 == 0 && i > 0 {
			e.SetInfo(&common.ClosedInterval{End: common.GetGlobalSeqNum()})
		} else {
			e.SetInfo(common.NextGlobalSeqNum())
		}
		n := common.GPool.Alloc(uint64(len(buf)))
		n.Buf = n.Buf[:len(buf)]
		copy(n.GetBuf(), buf)
		e.UnmarshalFromNode(n, true)
		err := s.AppendEntry(e)
		assert.Nil(t, err)
		fwg.Add(1)
		ch <- e
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}
