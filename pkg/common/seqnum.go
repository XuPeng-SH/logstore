package common

import "sync/atomic"

var (
	GlobalSeqNum uint64 = 0
)

func NextGlobalSeqNum() uint64 {
	return atomic.AddUint64(&GlobalSeqNum, uint64(1))
}

func GetGlobalSeqNum() uint64 {
	return atomic.LoadUint64(&GlobalSeqNum)
}
