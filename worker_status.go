package mq

import "sync/atomic"

// Describes worker states.
const (
	statusStopped int32 = iota
	statusRunning
)

// Describes worker state: running or stopped and provides an ability to change it atomically.
type workerStatus struct {
	value int32
}

// markAsRunning changes status to running.
func (status *workerStatus) markAsRunning() {
	atomic.StoreInt32(&status.value, statusRunning)
}

// markAsStoppedIfCan changes status to stopped if current status is running.
// Returns true on success if status was changed to stopped
// or false if status is already changed to stopped.
func (status *workerStatus) markAsStoppedIfCan() bool {
	return atomic.CompareAndSwapInt32(&status.value, statusRunning, statusStopped)
}
