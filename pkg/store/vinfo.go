package store

import (
	"fmt"
	"logstore/pkg/common"
)

type vInfo struct {
	commits     common.ClosedInterval
	checkpoints []common.ClosedInterval
}

func newVInfo() *vInfo {
	return &vInfo{
		checkpoints: make([]common.ClosedInterval, 0),
	}
}

func (info *vInfo) String() string {
	s := fmt.Sprintf("(%s | ", info.commits.String())
	for _, ckp := range info.checkpoints {
		s = fmt.Sprintf("%s%s", s, ckp.String())
	}
	s = fmt.Sprintf("%s)", s)
	return s
}

func (info *vInfo) Log(v interface{}) error {
	if v == nil {
		return nil
	}
	switch vi := v.(type) {
	case uint64:
		return info.LogCommit(vi)
	case *common.ClosedInterval:
		return info.LogCheckpoint(*vi)
	}
	panic("not supported")
}

func (info *vInfo) LogCommit(id uint64) error {
	return info.commits.Append(id)
}

func (info *vInfo) LogCheckpoint(interval common.ClosedInterval) error {
	if len(info.checkpoints) == 0 {
		info.checkpoints = append(info.checkpoints, interval)
		return nil
	}
	ok := info.checkpoints[len(info.checkpoints)-1].TryMerge(interval)
	if !ok {
		info.checkpoints = append(info.checkpoints, interval)
	}
	return nil
}
