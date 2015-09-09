package framework

import (
	"container/ring"
	"fmt"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	kafka "github.com/stealthly/go_kafka_client"
	"hash/fnv"
	"reflect"
)

type hashArray []*kafka.TopicAndPartition

func (s hashArray) Len() int      { return len(s) }
func (s hashArray) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s hashArray) Less(i, j int) bool {
	return hash(s[i].String()) < hash(s[j].String())
}

func hash(s string) int32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int32(h.Sum32())
}

func circularIterator(src interface{}) *ring.Ring {
	arr := reflect.ValueOf(src).Elem()
	circle := ring.New(arr.Len())
	for i := 0; i < arr.Len(); i++ {
		circle.Value = arr.Index(i).Interface()
		circle = circle.Next()
	}

	return circle
}

func position(haystack interface{}, needle interface{}) int {
	rSrc := reflect.ValueOf(haystack).Elem()
	for position := 0; position < rSrc.Len(); position++ {
		if reflect.DeepEqual(rSrc.Index(position).Interface(), needle) {
			return position
		}
	}

	return -1
}

func getScalarResources(offer *mesos.Offer, resourceName string) float64 {
	resources := 0.0
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources += res.GetScalar().GetValue()
	}
	return resources
}

func getRangeResources(offer *mesos.Offer, resourceName string) []*mesos.Value_Range {
	resources := make([]*mesos.Value_Range, 0)
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == resourceName
	})
	for _, res := range filteredResources {
		resources = append(resources, res.GetRanges().GetRange()...)
	}
	return resources
}

type intArray []int32

func (s intArray) Len() int           { return len(s) }
func (s intArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s intArray) Less(i, j int) bool { return s[i] < s[j] }

type byName []kafka.ConsumerThreadId

func (a byName) Len() int      { return len(a) }
func (a byName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool {
	this := fmt.Sprintf("%s-%d", a[i].Consumer, a[i].ThreadId)
	that := fmt.Sprintf("%s-%d", a[j].Consumer, a[j].ThreadId)
	return this < that
}
