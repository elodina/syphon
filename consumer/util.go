package consumer

import "sync"

type TopicAndPartition struct {
	Topic     string
	Partition int32
}

type TopicAndPartitionSet struct {
	internal map[TopicAndPartition]bool
}

func NewTopicAndPartitionSet() *TopicAndPartitionSet {
	return &TopicAndPartitionSet{
		internal: make(map[TopicAndPartition]bool),
	}
}

func (this *TopicAndPartitionSet) Contains(tp TopicAndPartition) bool {
	_, exists := this.internal[tp]
	return exists
}

func (this *TopicAndPartitionSet) Add(tp TopicAndPartition) bool {
	exists := this.Contains(tp)
	if exists {
		this.internal[tp] = true
	}

	return exists
}

func (this *TopicAndPartitionSet) Remove(tp TopicAndPartition) bool {
	exists := this.Contains(tp)
	if exists {
		delete(this.internal, tp)
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

func (this *TopicAndPartitionSet) GetArray() []TopicAndPartition {
	result := make([]TopicAndPartition, 0)
	for tp, _ := range this.internal {
		result = append(result, tp)
	}

	return result
}

func (this *TopicAndPartitionSet) IsEmpty() bool {
	return len(this.internal) == 0
}

func inLock(lock *sync.Mutex, fun func()) {
	lock.Lock()
	defer lock.Unlock()

	fun()
}
