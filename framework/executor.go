package framework

import (
	"fmt"
	"github.com/elodina/syphon/consumer"
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
    "encoding/json"
)

type HttpMirrorExecutor struct {
	partitionConsumer *consumer.PartitionConsumer
}

// Creates a new HttpMirrorExecutor with a given config.
func NewHttpMirrorExecutor() *HttpMirrorExecutor {
	return &HttpMirrorExecutor{}
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

// mesos.Executor interface method.
// Invoked when a fatal error has occured with the executor and/or executor driver.
func (this *HttpMirrorExecutor) Add(tps []*consumer.TopicAndPartition) {
    fmt.Printf("Got error message: %s\n", err)
}
