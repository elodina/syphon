package framework

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/elodina/syphon/consumer"
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/stealthly/siesta"
	"io/ioutil"
	"log"
	"net/http"
    "errors"
)

type HttpMirrorExecutor struct {
	partitionConsumer *consumer.PartitionConsumer
	httpsClient       *http.Client
	targetURL         string
}

// Creates a new HttpMirrorExecutor with a given config.
func NewHttpMirrorExecutor(certFile, keyFile, caFile, targetURL string) *HttpMirrorExecutor {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	httpsClient := &http.Client{Transport: transport}

	return &HttpMirrorExecutor{
		httpsClient: httpsClient,
		targetURL:   targetURL,
	}
}

// mesos.Executor interface method.
// Invoked once the executor driver has been able to successfully connect with Mesos.
// Not used by HttpMirrorExecutor yet.
func (this *HttpMirrorExecutor) Registered(driver executor.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Printf("Registered Executor on slave %s\n", slaveInfo.GetHostname())
}

// mesos.Executor interface method.
// Invoked when the executor re-registers with a restarted slave.
func (this *HttpMirrorExecutor) Reregistered(driver executor.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Printf("Re-registered Executor on slave %s\n", slaveInfo.GetHostname())
}

// mesos.Executor interface method.
// Invoked when the executor becomes "disconnected" from the slave.
func (this *HttpMirrorExecutor) Disconnected(executor.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

// mesos.Executor interface method.
// Invoked when a task has been launched on this executor.
func (this *HttpMirrorExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Printf("Launching task %s with command %s\n", taskInfo.GetName(), taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	config := &consumer.PartitionConsumerConfig{}
	json.Unmarshal(taskInfo.Data, config)
	this.partitionConsumer = consumer.NewPartitionConsumer(*config)

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		fmt.Printf("Failed to send status update: %s\n", runStatus)
	}
}

// mesos.Executor interface method.
// Invoked when a task running within this executor has been killed.
func (this *HttpMirrorExecutor) KillTask(_ executor.ExecutorDriver, taskId *mesos.TaskID) {
}

// mesos.Executor interface method.
// Invoked when a framework message has arrived for this executor.
func (this *HttpMirrorExecutor) FrameworkMessage(driver executor.ExecutorDriver, msg string) {
	fmt.Printf("Got framework message: %s\n", msg)
}

// mesos.Executor interface method.
// Invoked when the executor should terminate all of its currently running tasks.
func (this *HttpMirrorExecutor) Shutdown(executor.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

// mesos.Executor interface method.
// Invoked when a fatal error has occured with the executor and/or executor driver.
func (this *HttpMirrorExecutor) Error(driver executor.ExecutorDriver, err string) {
	fmt.Printf("Got error message: %s\n", err)
}

func (this *HttpMirrorExecutor) Assign(tps []consumer.TopicAndPartition) {
	tpSet := this.partitionConsumer.GetTopicPartitions()
	tpSet.RemoveAll(tps)
	for _, tp := range tpSet.GetArray() {
		this.partitionConsumer.Remove(tp.Topic, tp.Partition)
	}

	for _, tp := range tps {
		this.partitionConsumer.Add(tp.Topic, tp.Partition, this.MirrorMessage)
	}
}

func (this *HttpMirrorExecutor) MirrorMessage(topic string, partition int32, messages []*siesta.MessageAndOffset) error {
	encodedMessage, err := json.Marshal(EncodeMessage(topic, partition, messages))
	if err != nil {
		return err
	}
    request, err := http.NewRequest("POST", this.targetURL, bytes.NewReader(encodedMessage))
    if err != nil {
        return err
    }
	resp, err := this.httpsClient.Do(request)
    if err != nil {
        return err
    }
    if resp.StatusCode != 200 {
        defer resp.Body.Close()
        bodyData, err := ioutil.ReadAll(resp.Body)
        if err != nil {
            return err
        }

        return errors.New(string(bodyData))
    }

	return nil
}

type TransferMessage struct {
	Topic     string
	Partition int32
	Data      []byte
}

func EncodeMessage(topic string, partition int32, messages []*siesta.MessageAndOffset) []*TransferMessage {
	msgs := make([]*TransferMessage, 0)
	for _, message := range messages {
		if message.Message.Nested != nil && len(message.Message.Nested) > 0 {
			msgs = append(msgs, EncodeMessage(topic, partition, message.Message.Nested)...)
		} else {
			msgs = append(msgs, &TransferMessage{
				Topic:     topic,
				Partition: partition,
				Data:      message.Message.Value,
			})
		}
	}

	return msgs
}