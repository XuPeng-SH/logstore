package entry

import (
	"bytes"
	"logstore/pkg/common"
	"testing"
	"time"
)

func TestBase(t *testing.T) {
	now := time.Now()
	var buffer bytes.Buffer
	buffer.WriteString("helloworld")
	buffer.WriteString("helloworld")
	buffer.WriteString("helloworld")
	buf := buffer.Bytes()
	for i := 0; i < 100000; i++ {
		e := GetBase()
		e.SetType(ETFlush)
		n := common.GPool.Alloc(30)
		copy(n.GetBuf(), buf)
		e.UnmarshalFromNode(n, true)
		e.Free()
	}
	t.Logf("takes %s", time.Since(now))
	t.Log(common.GPool.String())
}
