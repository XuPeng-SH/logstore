package store

type fileAppender struct {
	rfile         *rotateFile
	activeId      uint64
	capacity      int
	size          int
	rollbackState *vFileState
	syncWaited    *vFile
}

func newFileAppender(rfile *rotateFile) *fileAppender {
	appender := &fileAppender{
		rfile: rfile,
	}
	return appender
}

func (appender *fileAppender) Prepare(size int) error {
	var err error
	appender.capacity = size
	appender.rfile.Lock()
	defer appender.rfile.Unlock()
	if appender.syncWaited, appender.rollbackState, err = appender.rfile.makeSpace(size); err != nil {
		return err
	}
	// appender.activeId = appender.rfile.idAlloc.Alloc()
	return err
}

func (appender *fileAppender) Write(data []byte) (int, error) {
	appender.size += len(data)
	if appender.size > appender.capacity {
		panic("write logic error")
	}
	n, err := appender.rollbackState.file.WriteAt(data,
		int64(appender.size-len(data)+appender.rollbackState.pos))
	return n, err
}

func (appender *fileAppender) Commit() error {
	appender.rollbackState.file.FinishWrite()
	return nil
}

func (appender *fileAppender) Rollback() {
	appender.rollbackState.file.FinishWrite()
	appender.Revert()
}

func (appender *fileAppender) Sync() error {
	if appender.size != appender.capacity {
		panic("write logic error")
	}
	if appender.syncWaited != nil {
		// fmt.Printf("Sync Waiting %s\n", appender.syncWaited.Name())
		appender.syncWaited.WaitCommitted()
	}
	return appender.rollbackState.file.Sync()
}

func (appender *fileAppender) Revert() {
	if err := appender.rollbackState.file.Truncate(int64(appender.rollbackState.pos)); err != nil {
		panic(err)
	}
}
