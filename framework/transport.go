package framework

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"time"
)

type ElodinaTransport struct {
	task           *mesos.TaskInfo
	staleThreshold time.Duration
	assignment     map[string]map[int]*PartitionState
}

func NewElodinaTransport(task *mesos.TaskInfo, staleThreshold time.Duration) *ElodinaTransport {
	return &ElodinaTransport{
		task:           task,
		staleThreshold: staleThreshold,
		assignment:     make(map[string]map[int]*PartitionState),
	}
}

func (this *ElodinaTransport) GetStalePartitions() []*PartitionState {
	stalePartitions := make([]*PartitionState, 0)
	for _, partitions := range this.assignment {
		for _, partition := range partitions {
			if partition.IsStale(this.staleThreshold) {
				stalePartitions = append(stalePartitions, partition)
			}
		}
	}

	return stalePartitions
}

func (this *ElodinaTransport) GetTask() *mesos.TaskInfo {
	return this.task
}

func (this *ElodinaTransport) GetAssignment() map[string]map[int]*PartitionState {
	return this.assignment
}

func (this *ElodinaTransport) UpdateOffset(topic string, partition int, offset int) {
	if _, ok := this.assignment[topic]; !ok {
		this.assignment[topic] = make(map[int]*PartitionState)
	}
	if _, ok := this.assignment[topic][partition]; !ok {
		this.assignment[topic][partition] = NewPartitionState(topic, partition)
	}

	this.assignment[topic][partition].Update(offset)
}

type PartitionState struct {
	topic       string
	partition   int
	lastUpdated int64
	offset      int
}

func NewPartitionState(topic string, partition int) *PartitionState {
	return &PartitionState{
		topic:     topic,
		partition: partition,
	}
}

func (this *PartitionState) Update(offset int) {
	this.offset = offset
	this.lastUpdated = time.Now().Unix()
}

func (this *PartitionState) IsStale(threshold time.Duration) bool {
	return time.Now().Sub(time.Unix(this.lastUpdated, 0)) >= threshold
}
