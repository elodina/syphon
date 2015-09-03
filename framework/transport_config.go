package framework

import "time"

type ElodinaTransportConfig struct {
	/* A string that uniquely identifies a set of consumers within the same consumer group */
	Groupid string

	/* A string that uniquely identifies a consumer within a group. Generated automatically if not set.
	   Set this explicitly for only testing purpose. */
	Consumerid string

	/* The socket timeout for network requests. Its value should be at least FetchWaitMaxMs. */
	SocketTimeout time.Duration

	/* The maximum number of bytes to attempt to fetch */
	FetchMessageMaxBytes int32

	/* The number of goroutines used to fetch data */
	NumConsumerFetchers int

	/* Max number of message batches buffered for consumption, each batch can be up to FetchBatchSize */
	QueuedMaxMessages int32

	/* Max number of retries during rebalance */
	RebalanceMaxRetries int32

	/* The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
	FetchMinBytes int32

	/* The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy FetchMinBytes */
	FetchWaitMaxMs int32

	/* Backoff time between retries during rebalance */
	RebalanceBackoff time.Duration

	/* Backoff time to refresh the leader of a partition after it loses the current leader */
	RefreshLeaderBackoff time.Duration

	/* Retry the offset commit up to this many times on failure. */
	OffsetsCommitMaxRetries int

	/* Try to commit offset every OffsetCommitInterval. If previous offset commit for a partition is still in progress updates the next offset to commit and continues.
	   This way it does not commit all the offset history if the coordinator is slow, but only the highest offsets. */
	OffsetCommitInterval time.Duration

	/* What to do if an offset is out of range.
	   SmallestOffset : automatically reset the offset to the smallest offset.
	   LargestOffset : automatically reset the offset to the largest offset.
	   Defaults to LargestOffset. */
	AutoOffsetReset int

	/* Client id is specified by the kafka consumer client, used to distinguish different clients. */
	Clientid string

	/* Whether messages from internal topics (such as offsets) should be exposed to the consumer. */
	ExcludeInternalTopics bool

	/* Select a strategy for assigning partitions to consumer streams. Possible values: RangeStrategy, RoundRobinStrategy */
	PartitionAssignmentStrategy string

	/* Amount of workers per partition to process consumed messages. */
	NumWorkers int

	/* Times to retry processing a failed message by a worker. */
	MaxWorkerRetries int

	/* Worker retry threshold. Increments each time a worker fails to process a message within MaxWorkerRetries.
	   When this threshold is hit within a WorkerThresholdTimeWindow, WorkerFailureCallback is called letting the user to decide whether the consumer should stop. */
	WorkerRetryThreshold int32

	/* Resets WorkerRetryThreshold if it isn't hit within this period. */
	WorkerThresholdTimeWindow time.Duration

	/* Worker timeout to process a single message. */
	WorkerTaskTimeout time.Duration

	/* Backoff between worker attempts to process a single message. */
	WorkerBackoff time.Duration

	/* Maximum wait time to gracefully stop a worker manager */
	WorkerManagersStopTimeout time.Duration

	/* Number of messages to accumulate before flushing them to workers */
	FetchBatchSize int

	/* Timeout to accumulate messages. Flushes accumulated batch to workers even if it is not yet full.
	   Resets after each flush meaning this won't be triggered if FetchBatchSize is reached before timeout. */
	FetchBatchTimeout time.Duration

	/* Backoff between fetch requests if no messages were fetched from a previous fetch. */
	RequeueAskNextBackoff time.Duration

	/* Buffer size for ask next channel. This value shouldn't be less than number of partitions per fetch routine. */
	AskNextChannelSize int

	/* Maximum fetch retries if no messages were fetched from a previous fetch */
	FetchMaxRetries int

	/* Maximum retries to fetch topic metadata from one broker. */
	FetchTopicMetadataRetries int

	/* Backoff for fetch topic metadata request if the previous request failed. */
	FetchTopicMetadataBackoff time.Duration

	/* Backoff between two fetch requests for one fetch routine. Needed to prevent fetcher from querying the broker too frequently. */
	FetchRequestBackoff time.Duration

	/* Indicates whether the client supports blue-green deployment.
	   This config entry is needed because blue-green deployment won't work with RoundRobin partition assignment strategy.
	   Defaults to true. */
	BlueGreenDeploymentEnabled bool

	/* Time to wait after consumer has registered itself in group */
	DeploymentTimeout time.Duration

	/* Service coordinator barrier timeout */
	BarrierTimeout time.Duration

	/* Flag for debug mode */
	Debug bool

	/* Metrics Prefix if the client wants to organize the way metric names are emitted. (optional) */
	MetricsPrefix string

	/* URL to send messages to */
	DestinationURL string

	/* Service coordinator URL */
	CoordinatorURL string

	/* URL pointing to Offset storage */
	OffsetStorageURL string

	/* Topic-Partition assignment */
	Assignment map[string][]int
}

//DefaultConsumerConfig creates a ConsumerConfig with sane defaults. Note that several required config entries (like Strategy and callbacks) are still not set.
func NewMirrorConfig(destinationURL string, coordinatorURL string, offsetStorageURL string,
	group string, assignment map[string][]int) *ElodinaTransportConfig {
	config := &ElodinaTransportConfig{}
	config.DestinationURL = destinationURL
	config.Groupid = group
	config.CoordinatorURL = coordinatorURL
	config.OffsetStorageURL = offsetStorageURL
	config.Assignment = assignment

	config.SocketTimeout = 30 * time.Second
	config.FetchMessageMaxBytes = 1024 * 1024
	config.NumConsumerFetchers = 1
	config.QueuedMaxMessages = 3
	config.RebalanceMaxRetries = 4
	config.FetchMinBytes = 1
	config.FetchWaitMaxMs = 100
	config.RebalanceBackoff = 5 * time.Second
	config.RefreshLeaderBackoff = 200 * time.Millisecond
	config.OffsetsCommitMaxRetries = 5
	config.OffsetCommitInterval = 3 * time.Second

	config.AutoOffsetReset = -1
	config.ExcludeInternalTopics = true

	config.NumWorkers = 10
	config.MaxWorkerRetries = 3
	config.WorkerRetryThreshold = 100
	config.WorkerThresholdTimeWindow = 1 * time.Minute
	config.WorkerBackoff = 500 * time.Millisecond
	config.WorkerTaskTimeout = 1 * time.Minute
	config.WorkerManagersStopTimeout = 1 * time.Minute

	config.FetchBatchSize = 100
	config.FetchBatchTimeout = 5 * time.Second

	config.FetchMaxRetries = 5
	config.RequeueAskNextBackoff = 5 * time.Second
	config.AskNextChannelSize = 1000
	config.FetchTopicMetadataRetries = 3
	config.FetchTopicMetadataBackoff = 1 * time.Second
	config.FetchRequestBackoff = 10 * time.Millisecond

	config.BlueGreenDeploymentEnabled = true
	config.DeploymentTimeout = 0 * time.Second
	config.BarrierTimeout = 30 * time.Second

	return config
}
