package common

import (
	"errors"
	"fmt"
)

var (
	IntervalNotContinousErr = errors.New("interval not continous")
	IntervalInvalidErr      = errors.New("invalid interval")
)

type ClosedInterval struct {
	Start, End uint64
}

func (i *ClosedInterval) String() string {
	return fmt.Sprintf("[%d,%d]", i.Start, i.End)
}

func (i *ClosedInterval) Append(id uint64) error {
	if i.Start == i.End && i.Start == 0 {
		i.Start = id
		i.End = id
		return nil
	}
	if id != i.End+1 {
		return IntervalInvalidErr
	}
	i.End = id
	return nil
}

func (i *ClosedInterval) Contains(o ClosedInterval) bool {
	if i == nil {
		return false
	}
	return i.Start <= o.Start && i.End >= o.End
}

func (i *ClosedInterval) TryMerge(o ClosedInterval) bool {
	if o.Start > i.End+1 || i.Start > o.End+1 {
		return false
	}
	if i.Start > o.Start {
		i.Start = o.Start
	}
	if i.End < o.End {
		i.End = o.End
	}

	return true
}
