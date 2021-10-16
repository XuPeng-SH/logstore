package main

import (
	"bytes"
	"fmt"
	"logstore/cmd/walexample/sm"
	"logstore/pkg/common"
	"logstore/pkg/store"
	"os"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
)

func main() {
	dir := "/tmp/walexample"
	os.RemoveAll(dir)
	checker := store.NewMaxSizeRotateChecker(int(common.K) * 10)
	cfg := &store.StoreCfg{
		RotateChecker: checker,
	}
	machine, err := sm.NewSimpleStateMachine(dir, cfg)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(100)

	lsn := uint64(0)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		insert := func() {
			defer wg.Done()
			var bs bytes.Buffer
			i := common.NextGlobalSeqNum()
			bs.WriteString(fmt.Sprintf("request-%d", i))
			r := &sm.Request{
				Op:   sm.TInsert,
				Data: bs.Bytes(),
			}
			if err = machine.OnRequest(r); err != nil {
				panic(err)
			}
			bs.Reset()
		}
		wg.Add(1)
		pool.Submit(insert)
		currLsn := machine.VisibleLSN()
		if currLsn != lsn {
			log.Infof("VisibleLSN %d, duration %s", currLsn, time.Since(now))
			lsn = currLsn
		}
	}

	wg.Wait()
	machine.Close()
	log.Infof("It takes %s", time.Since(now))
	currLsn := machine.VisibleLSN()
	log.Infof("VisibleLSN %d", currLsn)
}
