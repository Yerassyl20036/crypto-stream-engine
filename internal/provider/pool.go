package provider

import (
	"sync"
	"github.com/razedwell/crypto-stream-engine/internal/domain"
)

// TickPool manages a pool of reusable Tick objects to reduce GC pressure
var tickPool = sync.Pool{
	New: func() any {
		return &domain.Tick{}
	},
}

// AcquireTick gets a Tick from the pool
func AcquireTick() *domain.Tick {
	return tickPool.Get().(*domain.Tick)
}

// ReleaseTick returns a Tick to the pool after resetting it
func ReleaseTick(t *domain.Tick) {
	if t == nil {
		return
	}
	t.Reset()
	tickPool.Put(t)
}

// BatchReleaseTicks returns multiple ticks to the pool
func BatchReleaseTicks(ticks []*domain.Tick) {
	for _, t := range ticks {
		ReleaseTick(t)
	}
}