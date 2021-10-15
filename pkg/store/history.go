package store

import (
	"errors"
	"fmt"
	"io"
	"logstore/pkg/common"
	"sync"
)

var (
	HistoryEntryNotFoundErr = errors.New("aoe: history not found")
)

type HistoryFactory func() History

var (
	DefaultHistoryFactory = func() History {
		return newHistory(nil)
	}
)

type history struct {
	mu      *sync.RWMutex
	entries []VFile
}

func newHistory(mu *sync.RWMutex) *history {
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	return &history{
		mu: mu,
	}
}

func (h *history) String() string {
	s := fmt.Sprintf("{")
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, entry := range h.entries {
		s = fmt.Sprintf("%s\n%s", s, entry.String())
	}
	if len(h.entries) > 0 {
		s = fmt.Sprintf("%s\n}", s)
	} else {
		s = fmt.Sprintf("%s}", s)
	}
	return s
}

func (h *history) OldestEntry() VFile {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.entries) == 0 {
		return nil
	}
	return h.entries[0]
}

func (h *history) Entries() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func (h *history) Empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries) == 0
}

func (h *history) findEntry(id int) (int, VFile) {
	for idx, entry := range h.entries {
		if entry.Id() == id {
			return idx, entry
		}
	}
	return 0, nil
}

func (h *history) DropEntry(id int) (VFile, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx, entry := h.findEntry(id)
	if entry == nil {
		return nil, HistoryEntryNotFoundErr
	}
	h.entries = append(h.entries[:idx], h.entries[idx+1:]...)
	return entry, entry.Destroy()
}

func (h *history) Extend(entries ...VFile) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, entries...)
}

func (h *history) Append(entry VFile) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, entry)
}

func (h *history) GetEntry(id int) VFile {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, e := h.findEntry(id)
	return e
}

func (h *history) EntryIds() []int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]int, len(h.entries))
	for idx, entry := range h.entries {
		ids[idx] = entry.Id()
	}
	return ids
}

type entryWrapper struct {
	offset int
	entry  VFile
}

// One worker
func (h *history) TryTruncate() error {
	var gInterval *common.ClosedInterval
	toDelete := make([]entryWrapper, 0, 4)
	h.mu.RLock()
	entries := make([]VFile, len(h.entries))
	for i, entry := range h.entries {
		entries[i] = entry
	}
	h.mu.RUnlock()
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		wrapper := entryWrapper{entry: e}
		if e.InCommits(gInterval) && e.InCheckpoint(gInterval) {
			wrapper.offset = i
			toDelete = append(toDelete, wrapper)
			continue
		}
		gInterval = e.MergeCheckpoint(gInterval)
	}
	h.mu.Lock()
	for _, wrapper := range toDelete {
		h.entries = append(h.entries[:wrapper.offset], h.entries[wrapper.offset+1:]...)
	}
	h.mu.Unlock()
	for _, wrapper := range toDelete {
		if err := wrapper.entry.Destroy(); err != nil {
			return err
		}
	}
	return nil
}

func (h *history) Replay(handle ReplayHandle, observer ReplayObserver) error {
	for _, entry := range h.entries {
		observer.OnNewEntry(entry.Id())
		for {
			if err := handle(entry, observer); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
		}
	}
	return nil
}
