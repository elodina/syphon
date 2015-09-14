package framework

import (
	"github.com/elodina/syphon/consumer"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"time"
)

type ElodinaTransport struct {
	connectUrl     string
	task           *mesos.TaskInfo
	staleThreshold time.Duration
	state          map[string]map[int]*PartitionState
	assignment     []consumer.TopicAndPartition
	pending        bool
}

func NewElodinaTransport(connectUrl string, task *mesos.TaskInfo, staleThreshold time.Duration) *ElodinaTransport {
	return &ElodinaTransport{
		connectUrl:     connectUrl,
		task:           task,
		staleThreshold: staleThreshold,
		state:          make(map[string]map[int]*PartitionState),
		assignment:     make([]consumer.TopicAndPartition, 0),
	}
}

func (this *ElodinaTransport) GetStalePartitions() []*PartitionState {
	stalePartitions := make([]*PartitionState, 0)
	for _, partitions := range this.state {
		for _, partition := range partitions {
			if partition.IsStale(this.staleThreshold) {
				stalePartitions = append(stalePartitions, partition)
			}
		}
	}

	return stalePartitions
}

func (this *ElodinaTransport) GetConnectUrl() string {
	return this.connectUrl
}

func (this *ElodinaTransport) GetTask() *mesos.TaskInfo {
	return this.task
}

func (this *ElodinaTransport) GetAssignment() []consumer.TopicAndPartition {
	return this.assignment
}

func (this *ElodinaTransport) GetState() map[string]map[int]*PartitionState {
	return this.state
}

func (this *ElodinaTransport) IsPending() bool {
	return this.pending
}

func (this *ElodinaTransport) UpdateOffset(topic string, partition int, offset int) {
	if _, ok := this.state[topic]; !ok {
		this.state[topic] = make(map[int]*PartitionState)
	}
	if _, ok := this.state[topic][partition]; !ok {
		this.state[topic][partition] = NewPartitionState(topic, partition)
	}

	this.state[topic][partition].Update(offset)
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
