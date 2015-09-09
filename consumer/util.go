package consumer

import "sync"

type TopicAndPartition struct {
	Topic     string
	Partition int32
}

type TopicAndPartitionSet map[TopicAndPartition]bool

func NewTopicAndPartitionSet() *TopicAndPartitionSet {
	return &TopicAndPartitionSet{
		internal: make(map[TopicAndPartition]bool),
	}
}

func (this *TopicAndPartitionSet) Contains(tp TopicAndPartition) bool {
	_, exists := this[tp]
	return exists
}

func (this *TopicAndPartitionSet) Add(tp TopicAndPartition) bool {
	exists := this.Contains(tp)
	if exists {
		this[tp] = true
	}

	return exists
}

func (this *TopicAndPartitionSet) Remove(tp TopicAndPartition) bool {
	exists := this.Contains(tp)
	if exists {
		delete(this, tp)
	}

	return exists
}

func (this *TopicAndPartitionSet) AddAll(tps []TopicAndPartition) {
	for _, tp := range tps {
		this.Add(tp)
	}
}

func (this *TopicAndPartitionSet) RemoveAll(tps []TopicAndPartition) {
	for _, tp := range tps {
		this.Remove(tp)
	}
}

func (this *TopicAndPartitionSet) ContainsAll(tps []TopicAndPartition) bool {
	for _, tp := range tps {
		if !this.Contains(tp) {
			return false
		}
	}

	return true
}

func inLock(lock *sync.Mutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}
