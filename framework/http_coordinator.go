package framework

import (
	"net/http"
	"time"
    "fmt"
    "encoding/json"
    "bytes"
    "sort"
    "io"
    "strconv"
    "io/ioutil"
    kafka "github.com/stealthly/go_kafka_client"
)

var (
    consumersPath    = "/consumers"
    brokerIdsPath    = "/brokers/ids"
    brokerTopicsPath = "/brokers/topics"
)

type HttpCoordinator struct {
	config        *HttpCoordinatorConfig
	httpClient    *http.Client
	httpTransport *http.Transport
}

/* ZookeeperConfig is used to pass multiple configuration entries to ZookeeperCoordinator. */
type HttpCoordinatorConfig struct {
	/* Coordinator URL */
	ConnectURL string

	/* Connection Timeout value */
	Timeout time.Duration

	/* Max retries for any request except CommitOffset. CommitOffset is controlled by ConsumerConfig.OffsetsCommitMaxRetries. */
	MaxRequestRetries int

	/* Backoff to retry any request */
	RequestBackoff time.Duration
}

/* Created a new ZookeeperConfig with sane defaults. Default ZookeeperConnect points to localhost. */
func NewHttpCoordinatorConfig() *HttpCoordinatorConfig {
	config := &HttpCoordinatorConfig{}
	config.ConnectURL = "http://localhost:8080"
	config.Timeout = 1 * time.Second
	config.MaxRequestRetries = 3
	config.RequestBackoff = 150 * time.Millisecond

	return config
}

func NewHttpCoordinator(config HttpCoordinatorConfig) *HttpCoordinator {
	httpTransport := &http.Transport{}
	return &HttpCoordinator{
		config:        &config,
		httpTransport: httpTransport,
		httpClient: &http.Client{
			Transport: httpTransport,
			Timeout:   config.Timeout,
		},
	}
}

func (this *HttpCoordinator) Connect() error {
    //NOOP
	return nil
}

func (this *HttpCoordinator) Disconnect() {
    //NOOP
}

func (this *HttpCoordinator) RegisterConsumer(Consumerid string, Group string, TopicCount kafka.TopicsToNumStreams) error {
    topicCountBlob, err := json.Marshal(TopicCount)
    if err != nil {
        return err
    }

    _, err = this.doHttp("POST", fmt.Sprintf("%s%s", this.config.ConnectURL, consumersPath), topicCountBlob)
    return err
}

func (this *HttpCoordinator) DeregisterConsumer(Consumerid string, Group string) error {
    _, err := this.doHttp("DELETE", fmt.Sprintf("%s%s/%s/%s", this.config.ConnectURL, consumersPath, Group, Consumerid), nil)
    return err
}

func (this *HttpCoordinator) GetConsumerInfo(Consumerid string, Group string) (*kafka.ConsumerInfo, error) {
    response, err := this.doHttp("GET", fmt.Sprintf("%s%s/%s/%s", this.config.ConnectURL, consumersPath, Group, Consumerid), nil)
    if err != nil {
        return nil, err
    }

    consumerInfo := &kafka.ConsumerInfo{}
    err = this.decodeHttpResponse(response, consumerInfo)

    return consumerInfo, err
}

func (this *HttpCoordinator) GetConsumersPerTopic(Group string, ExcludeInternalTopics bool) (map[string][]kafka.ConsumerThreadId, error) {
    consumers, err := this.GetConsumersInGroup(Group)
    if err != nil {
        return nil, err
    }
    consumersPerTopicMap := make(map[string][]kafka.ConsumerThreadId)
    for _, consumer := range consumers {
        topicsToNumStreams, err := kafka.NewTopicsToNumStreams(Group, consumer, this, ExcludeInternalTopics)
        if err != nil {
            return nil, err
        }

        for topic, threadIds := range topicsToNumStreams.GetConsumerThreadIdsPerTopic() {
            for _, threadId := range threadIds {
                consumersPerTopicMap[topic] = append(consumersPerTopicMap[topic], threadId)
            }
        }
    }

    for topic := range consumersPerTopicMap {
        sort.Sort(byName(consumersPerTopicMap[topic]))
    }

    return consumersPerTopicMap, nil
}

func (this *HttpCoordinator) GetConsumersInGroup(Group string) ([]string, error) {
    response, err := this.doHttp("GET", fmt.Sprintf("%s%s/%s", this.config.ConnectURL, consumersPath, Group), nil)
    if err != nil {
        return nil, err
    }

    consumerIds := make([]string, 0)
    err = this.decodeHttpResponse(response, consumerIds)

    return consumerIds, err
}

func (this *HttpCoordinator) GetAllTopics() ([]string, error) {
    response, err := this.doHttp("GET", fmt.Sprintf("%s%s/%s", this.config.ConnectURL, brokerTopicsPath), nil)
    if err != nil {
        return nil, err
    }

    topics := make([]string, 0)
    err = this.decodeHttpResponse(response, topics)

    return topics, err
}

func (this *HttpCoordinator) GetPartitionsForTopics(Topics []string) (map[string][]int32, error) {
    result := make(map[string][]int32)
    partitionAssignments, err := this.getPartitionAssignmentsForTopics(Topics)
    if err != nil {
        return nil, err
    }
    for topic, partitionAssignment := range partitionAssignments {
        for partition, _ := range partitionAssignment {
            result[topic] = append(result[topic], partition)
        }
    }

    for topic, _ := range partitionAssignments {
        sort.Sort(intArray(result[topic]))
    }

    return result, nil
}

func (this *HttpCoordinator) GetAllBrokers() ([]*kafka.BrokerInfo, error) {
    response, err := this.doHttp("GET", fmt.Sprintf("%s%s/%s", this.config.ConnectURL, brokerIdsPath), nil)
    if err != nil {
        return nil, err
    }

    brokers := make([]*kafka.BrokerInfo, 0)
    err = this.decodeHttpResponse(response, brokers)

    return brokers, err
}

func (this *HttpCoordinator) SubscribeForChanges(Group string) (<-chan kafka.CoordinatorEvent, error) {
    //NOOP
    return nil, nil
}

func (this *HttpCoordinator) RequestBlueGreenDeployment(blue kafka.BlueGreenDeployment, green kafka.BlueGreenDeployment) error {
    //NOOP
    return nil
}

func (this *HttpCoordinator) GetBlueGreenRequest(Group string) (map[string]*kafka.BlueGreenDeployment, error) {
    //NOOP
    return nil, nil
}

func (this *HttpCoordinator) AwaitOnStateBarrier(consumerId string, group string, stateHash string, barrierSize int, api string, timeout time.Duration) bool {
    //NOOP
    return false
}

func (this *HttpCoordinator) RemoveStateBarrier(group string, stateHash string, api string) error {
    //NOOP
    return nil
}

func (this *HttpCoordinator) Unsubscribe() {
    //NOOP
}

func (this *HttpCoordinator) ClaimPartitionOwnership(Group string, Topic string, Partition int32, ConsumerThreadId kafka.ConsumerThreadId) (bool, error) {
    return true, nil
}

func (this *HttpCoordinator) ReleasePartitionOwnership(Group string, Topic string, Partition int32) error {
    //NOOP
    return nil
}

func (this *HttpCoordinator) RemoveOldApiRequests(group string) error {
    //NOOP
    return nil
}

func (this *HttpCoordinator) GetOffset(group string, topic string, partition int32) (int64, error) {
    response, err := this.doHttp("GET", fmt.Sprintf("%s%s/%s/offsets/%s/%d", this.config.ConnectURL, consumersPath, group, topic, partition), nil)
    defer response.Body.Close()
    if response.StatusCode != 200 {
        return 0, fmt.Errorf("HTTP requestuest failed with code %d: %v", response.StatusCode, response.Status)
    }
    rawBody, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return 0, err
    }
    offset, err := strconv.Atoi(string(rawBody))
    if err != nil {
        return 0, err
    }

    return int64(offset), err
}

func (this *HttpCoordinator) CommitOffset(group string, topic string, partition int32, offset int64) error {
    _, err := this.doHttp("PUT", fmt.Sprintf("%s%s/%s/offsets/%s/%d/%d", this.config.ConnectURL, consumersPath, group, topic, partition, offset), nil)
    return err
}

func (this *HttpCoordinator) getPartitionAssignmentsForTopics(topics []string) (map[string]map[int32][]int32, error) {
    result := make(map[string]map[int32][]int32)
    for _, topic := range topics {
        topicInfo, err := this.getTopicInfo(topic)
        if err != nil {
            return nil, err
        }
        result[topic] = make(map[int32][]int32)
        for partition, replicaIds := range topicInfo.Partitions {
            partitionInt, err := strconv.Atoi(partition)
            if err != nil {
                return nil, err
            }
            result[topic][int32(partitionInt)] = replicaIds
        }
    }

    return result, nil
}

func (this *HttpCoordinator) getTopicInfo(topic string) (*kafka.TopicInfo, error) {
    response, err := this.doHttp("GET", fmt.Sprintf("%s%s/%s/%s", this.config.ConnectURL, brokerTopicsPath, topic), nil)
    if err != nil {
        return nil, err
    }

    topicInfo := &kafka.TopicInfo{}
    err = this.decodeHttpResponse(response, topicInfo)

    return topicInfo, err
}

func (this *HttpCoordinator) doHttp(method string, url string, body []byte) (*http.Response, error) {
    var response *http.Response
    var requestErr error
    for i := 0; i < this.config.MaxRequestRetries; i++ {
        var reader io.Reader
        if body != nil {
            reader = bytes.NewReader(body)
        }
        request, err := http.NewRequest(method, url, reader)
        if err != nil {
            return nil, err
        }

        response, requestErr = this.httpClient.Do(request)
        if requestErr == nil {
            break
        }
    }

    return response, requestErr
}

func (this *HttpCoordinator) decodeHttpResponse(response *http.Response, result interface{}) error {
    defer response.Body.Close()
    if response.StatusCode != 200 {
        return fmt.Errorf("HTTP requestuest failed with code %d: %v", response.StatusCode, response.Status)
    }
    decoder := json.NewDecoder(response.Body)

    return decoder.Decode(result)
}