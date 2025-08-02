package internal

import (
	"sync"

	"github.com/INLOpen/nexusbase/levels"
)

type PrivateManagerStore interface {
	GetLogFilePath() string
}

type PrivateTagIndexManager interface {
	GetShutdownChain() chan struct{}
	GetWaitGroup() *sync.WaitGroup
	GetLevelsManager() levels.Manager
}

type PrivateLevelManager interface {
	SetBaseTargetSize(size int64)
	GetBaseTargetSize() int64
}

type PrivateStorageEngine interface {
}

type PrivateWAL interface {
	SetTestingOnlyInjectAppendError(err error)
}
