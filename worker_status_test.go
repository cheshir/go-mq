package mq

import (
	"testing"
)

func TestWorkerStatus_DefaultStatusIsStopped(t *testing.T) {
	status := workerStatus{}

	if status.value != statusStopped {
		t.Errorf("Expected status is %d but got %d", statusStopped, status.value)
	}
}

func TestWorkerStatus_MarkedAsRunning(t *testing.T) {
	status := workerStatus{}
	status.markAsRunning()

	if status.value != statusRunning {
		t.Errorf("Expected status is %d but got %d", statusRunning, status.value)
	}
}

func TestWorkerStatus_MarkedAsStopped_FromRunningStatus(t *testing.T) {
	status := workerStatus{value: statusRunning}
	status.markAsStoppedIfCan()

	if status.value != statusStopped {
		t.Errorf("Expected status is %d but got %d", statusStopped, status.value)
	}
}

func TestWorkerStatus_MarkedAsStopped_AlreadyStopped(t *testing.T) {
	status := workerStatus{}
	status.markAsStoppedIfCan()

	if status.value != statusStopped {
		t.Errorf("Expected status is %d but got %d", statusStopped, status.value)
	}
}
