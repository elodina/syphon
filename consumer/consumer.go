package consumer

import (
	"fmt"
	"github.com/stealthly/siesta"
	"sync"
	"sync/atomic"
	"time"
)

type PartitionConsumer struct {
	config       PartitionConsumerConfig
	kafkaClient  siesta.Connector
	fetchers     map[string]map[int32]*FetcherState
	fetchersLock sync.Mutex
}

type PartitionConsumerConfig struct {
	// Consumer group
	Group string

	//Interval to commit offsets at
	CommitInterval time.Duration

	// BrokerList is a bootstrap list to discover other brokers in a cluster. At least one broker is required.
	BrokerList []string

	// ReadTimeout is a timeout to read the response from a TCP socket.
	ReadTimeout time.Duration

	// WriteTimeout is a timeout to write the request to a TCP socket.
	WriteTimeout time.Duration

	// ConnectTimeout is a timeout to connect to a TCP socket.
	ConnectTimeout time.Duration

	// Sets whether the connection should be kept alive.
	KeepAlive bool

	// A keep alive period for a TCP connection.
	KeepAliveTimeout time.Duration

	// Maximum number of open connections for a connector.
	MaxConnections int

	// Maximum number of open connections for a single broker for a connector.
	MaxConnectionsPerBroker int

	// Maximum fetch size in bytes which will be used in all Consume() calls.
	FetchSize int32

	// The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block
	FetchMinBytes int32

	// The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy FetchMinBytes
	FetchMaxWaitTime int32

	// Number of retries to get topic metadata.
	MetadataRetries int

	// Backoff value between topic metadata requests.
	MetadataBackoff time.Duration

	// Number of retries to commit an offset.
	CommitOffsetRetries int

	// Backoff value between commit offset requests.
	CommitOffsetBackoff time.Duration

	// Number of retries to get consumer metadata.
	ConsumerMetadataRetries int

	// Backoff value between consumer metadata requests.
	ConsumerMetadataBackoff time.Duration

	// ClientID that will be used by a connector to identify client requests by broker.
	ClientID string
}

func NewPartitionConsumerConfig(group string) PartitionConsumerConfig {
	return PartitionConsumerConfig{
		Group:                   group,
		CommitInterval:          1 * time.Second,
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            5 * time.Second,
		ConnectTimeout:          5 * time.Second,
		KeepAlive:               true,
		KeepAliveTimeout:        1 * time.Minute,
		MaxConnections:          5,
		MaxConnectionsPerBroker: 5,
		FetchSize:               1024000,
		FetchMaxWaitTime:        1000,
		MetadataRetries:         5,
		MetadataBackoff:         200 * time.Millisecond,
		CommitOffsetRetries:     5,
		CommitOffsetBackoff:     200 * time.Millisecond,
		ConsumerMetadataRetries: 15,
		ConsumerMetadataBackoff: 500 * time.Millisecond,
		ClientID:                "partition-consumer",
	}
}

func NewPartitionConsumer(consumerConfig PartitionConsumerConfig) *PartitionConsumer {
	connectorConfig := siesta.NewConnectorConfig()
	connectorConfig.BrokerList = consumerConfig.BrokerList
	connectorConfig.ClientID = consumerConfig.ClientID
	connectorConfig.CommitOffsetBackoff = consumerConfig.CommitOffsetBackoff
	connectorConfig.CommitOffsetRetries = consumerConfig.CommitOffsetRetries
	connectorConfig.ConnectTimeout = consumerConfig.ConnectTimeout
	connectorConfig.ConsumerMetadataBackoff = consumerConfig.ConsumerMetadataBackoff
	connectorConfig.ConsumerMetadataRetries = consumerConfig.ConsumerMetadataRetries
	connectorConfig.FetchMaxWaitTime = consumerConfig.FetchMaxWaitTime
	connectorConfig.FetchMinBytes = consumerConfig.FetchMinBytes
	connectorConfig.FetchSize = consumerConfig.FetchSize
	connectorConfig.KeepAlive = consumerConfig.KeepAlive
	connectorConfig.KeepAliveTimeout = consumerConfig.KeepAliveTimeout
	connectorConfig.MaxConnections = consumerConfig.MaxConnections
	connectorConfig.MaxConnectionsPerBroker = consumerConfig.MaxConnectionsPerBroker
	connectorConfig.MetadataBackoff = consumerConfig.MetadataBackoff
	connectorConfig.MetadataRetries = consumerConfig.MetadataRetries
	connectorConfig.ReadTimeout = consumerConfig.ReadTimeout
	connectorConfig.WriteTimeout = consumerConfig.WriteTimeout
	kafkaClient, err := siesta.NewDefaultConnector(connectorConfig)
	if err != nil {
		panic(err)
	}

	consumer := &PartitionConsumer{
		config:      consumerConfig,
		kafkaClient: kafkaClient,
		fetchers:    make(map[string]map[int32]*FetcherState),
	}

	commitTimer := time.NewTimer(consumerConfig.CommitInterval)
	go func() {
		for {
			select {
			case <-commitTimer.C:
				{

					for topic, partitions := range consumer.fetchers {
						for partition, fetcherState := range partitions {
							offsetToCommit := fetcherState.GetOffset()
							if offsetToCommit > fetcherState.LastCommitted {
								err := consumer.kafkaClient.CommitOffset(consumer.config.Group, topic, partition, offsetToCommit)
								if err != nil {
									fmt.Println(err.Error())
								}
							}
							if fetcherState.Removed {
								inLock(&consumer.fetchersLock, func() {
									if consumer.fetchers[topic][partition].Removed {
										delete(consumer.fetchers[topic], partition)
									}
								})
							}
						}
					}
					commitTimer.Reset(consumerConfig.CommitInterval)
				}
			}
		}
	}()

	return consumer
}

func (this *PartitionConsumer) Add(topic string, partition int32, strategy Strategy) error {
	if _, exists := this.fetchers[topic]; !exists {
		this.fetchers[topic] = make(map[int32]*FetcherState)
	}
	var fetcherState *FetcherState
	inLock(&this.fetchersLock, func() {
		if _, exists := this.fetchers[topic][partition]; !exists || this.fetchers[topic][partition].Removed {
			if !exists {
				offset, err := this.kafkaClient.GetOffset(this.config.Group, topic, partition)
				if err != nil {
					//It's not critical, since offsets have not been committed yet
					fmt.Println(err.Error())
				}
				fetcherState := NewFetcherState(offset)
				this.fetchers[topic][partition] = fetcherState
			} else {
				this.fetchers[topic][partition].Removed = false
			}
		}
	})

	if fetcherState == nil {
		return nil
	}

	go func() {
		for {
			response, err := this.kafkaClient.Fetch(topic, partition, fetcherState.GetOffset()+1)
			select {
			case fetcherState.Removed = <-fetcherState.stopChannel:
				{
					break
				}
			default:
				{
					if _, exists := response.Data[topic]; !exists {
						continue
					}
					if _, exists := response.Data[topic][partition]; !exists {
						continue
					}
					if len(response.Data[topic][partition].Messages) == 0 {
						continue
					}

					err = strategy(topic, partition, response.Data[topic][partition].Messages)
					if err != nil {
						fmt.Println(err.Error())
					}

					offsetIndex := len(response.Data[topic][partition].Messages) - 1
					offsetValue := response.Data[topic][partition].Messages[offsetIndex].Offset
					fetcherState.SetOffset(offsetValue)
				}
			}
		}
	}()

	return nil
}

func (this *PartitionConsumer) Remove(topic string, partition int32) {
	if topicFetchers, exists := this.fetchers[topic]; exists {
		if fetcherState, exists := topicFetchers[partition]; exists {
			fetcherState.GetStopChannel() <- true
		}
	}
}

func (this *PartitionConsumer) GetTopicPartitions() *TopicAndPartitionSet {
	tpSet := NewTopicAndPartitionSet()
	for topic, partitions := range this.fetchers {
		for partition, _ := range partitions {
			tpSet.Add(TopicAndPartition{topic, partition})
		}
	}

	return tpSet
}

type FetcherState struct {
	LastCommitted int64
	Removed       bool
	offset        int64
	stopChannel   chan bool
}

func NewFetcherState(initialOffset int64) *FetcherState {
	return &FetcherState{
		LastCommitted: initialOffset,
		offset:        initialOffset,
		stopChannel:   make(chan bool),
	}
}

func (this *FetcherState) GetStopChannel() chan<- bool {
	return this.stopChannel
}

func (this *FetcherState) GetOffset() int64 {
	return atomic.LoadInt64(&this.offset)
}

func (this *FetcherState) SetOffset(offset int64) {
	atomic.StoreInt64(&this.offset, offset)
}

type Strategy func(topic string, partition int32, message []*siesta.MessageAndOffset) error
