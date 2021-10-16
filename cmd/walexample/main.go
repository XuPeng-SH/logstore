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

	lsn := uint64(0)
	var bs bytes.Buffer
	now := time.Now()
	for i := 0; i < 4000; i++ {
		bs.WriteString(fmt.Sprintf("request-%d", i))
		r := &sm.Request{
			Op:   sm.TInsert,
			Data: bs.Bytes(),
		}
		if err = machine.OnRequest(r); err != nil {
			panic(err)
		}
		currLsn := machine.VisibleLSN()
		if currLsn != lsn {
			// log.Infof("VisibleLSN %d, duration %s", currLsn, time.Since(now))
			lsn = currLsn
		}
		bs.Reset()
	}

	wg.Wait()
	machine.Close()
	log.Infof("It takes %s", time.Since(now))
	currLsn := machine.VisibleLSN()
	log.Infof("VisibleLSN %d", currLsn)
}
