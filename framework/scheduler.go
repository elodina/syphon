package framework

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
	kafka "github.com/stealthly/go_kafka_client"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type ElodinaTransportSchedulerConfig struct {
	Topics []string

	// Number of CPUs allocated for each created Mesos task.
	CpuPerTask float64

	// Number of RAM allocated for each created Mesos task.
	MemPerTask float64

	// Artifact server host name. Will be used to fetch the executor.
	ServiceHost string

	// Artifact server port.Will be used to fetch the executor.
	ServicePort int

	// Name of the executor archive file.
	ExecutorArchiveName string

	// Name of the executor binary file contained in the executor archive.
	ExecutorBinaryName string

	// Maximum retries to kill a task.
	KillTaskRetries int

	// Number of task instances to run.
	Instances int

	// time after partition is considered stale
	StaleDuration time.Duration

	// Mirror configuration
	transportConfig *ElodinaTransportConfig
}

func NewElodinaTransportSchedulerConfig() ElodinaTransportSchedulerConfig {
	return ElodinaTransportSchedulerConfig{
		CpuPerTask:      0.2,
		MemPerTask:      256,
		KillTaskRetries: 3,
	}
}

type ElodinaTransportScheduler struct {
	config            *ElodinaTransportSchedulerConfig
	runningInstances  int32
	taskIdToTaskState map[string]*ElodinaTransport
	toKill            []*ElodinaTransport
	coordinator       kafka.ConsumerCoordinator
}

func NewElodinaTransportScheduler(config ElodinaTransportSchedulerConfig) *ElodinaTransportScheduler {
	scheduler := &ElodinaTransportScheduler{
		config:            &config,
		taskIdToTaskState: make(map[string]*ElodinaTransport),
	}

	return scheduler
}

// mesos.Scheduler interface method.
// Invoked when the scheduler successfully registers with a Mesos master.
func (this *ElodinaTransportScheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesos.FrameworkID,
	masterInfo *mesos.MasterInfo) {
	fmt.Printf("Framework Registered with Master %s\n", masterInfo)
}

// mesos.Scheduler interface method.
// Invoked when the scheduler re-registers with a newly elected Mesos master.
func (this *ElodinaTransportScheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	fmt.Printf("Framework Re-Registered with Master %s\n", masterInfo)
}

// mesos.Scheduler interface method.
// Invoked when the scheduler becomes "disconnected" from the master.
func (this *ElodinaTransportScheduler) Disconnected(driver scheduler.SchedulerDriver) {
	fmt.Println("Disconnected")
}

// mesos.Scheduler interface method.
// Invoked when resources have been offered to this framework.
func (this *ElodinaTransportScheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	fmt.Println("Received offers")
	for _, transfer := range this.toKill {
		this.tryKillTask(driver, transfer.task.TaskId)
		this.removeTask(transfer.task.TaskId)
		this.decRunningInstances()
	}
	this.toKill = make([]*ElodinaTransport, 0)

	offersAndTasks := make(map[*mesos.Offer][]*mesos.TaskInfo)
	transports := make([]*ElodinaTransport, 0)
	for _, offer := range offers {
		cpus := getScalarResources(offer, "cpus")
		memory := getScalarResources(offer, "mem")
		ports := getRangeResources(offer, "ports")

		remainingCpus := cpus
		remainingMemory := memory
		var tasks []*mesos.TaskInfo
		for !this.hasEnoughInstances() && this.hasEnoughCpuAndMemory(cpus, memory) {
			port := this.takePort(&ports)
			taskPort := &mesos.Value_Range{Begin: port, End: port}
			taskId := &mesos.TaskID{
				Value: proto.String(fmt.Sprintf("elodina-mirror-%s-%d", *offer.Hostname, *port)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String(taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: this.createExecutor(this.runningInstances, *port),
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", float64(this.config.CpuPerTask)),
					util.NewScalarResource("mem", float64(this.config.MemPerTask)),
					util.NewRangesResource("ports", []*mesos.Value_Range{taskPort}),
				},
			}
			fmt.Printf("Prepared task: %s with offer %s for launch. Ports: %s\n", task.GetName(), offer.Id.GetValue(), taskPort)

			ports = ports[1:]
			remainingCpus -= this.config.CpuPerTask
			remainingMemory -= this.config.MemPerTask
			this.incRunningInstances()

			tasks = append(tasks, task)
			transport := NewElodinaTransport(task, this.config.StaleDuration)
			transports = append(transports, transport)
			this.taskIdToTaskState[*taskId.Value] = transport
		}
		offersAndTasks[offer] = tasks
	}

	this.assignPartitions(transports)

	unlaunchedTasks := this.config.Instances - int(this.getRunningInstances())
	if unlaunchedTasks > 0 {
		fmt.Printf("There are still %d tasks to be launched and no more resources are available.", unlaunchedTasks)
	}

	for _, offer := range offers {
		tasks := offersAndTasks[offer]
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

// mesos.Scheduler interface method.
// Invoked when the status of a task has changed.
func (this *ElodinaTransportScheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	fmt.Printf("Status update from executor %s task %s on slave %s: %s\n",
		*status.GetExecutorId().Value, status.GetState().String(), *status.GetSlaveId().Value, string(status.GetData()))
}

// mesos.Scheduler interface method.
// Invoked when an offer is no longer valid.
func (this *ElodinaTransportScheduler) OfferRescinded(driver scheduler.SchedulerDriver, offerId *mesos.OfferID) {
	fmt.Printf("Offer %s is no longer valid\n", *offerId.Value)
}

// mesos.Scheduler interface method.
// Invoked when an executor sends a message.
func (this *ElodinaTransportScheduler) FrameworkMessage(driver scheduler.SchedulerDriver, executorId *mesos.ExecutorID,
	slaveId *mesos.SlaveID, message string) {
	fmt.Printf("Message from executor %s: %s\n", *executorId.Value, message)
}

// mesos.Scheduler interface method.
// Invoked when a slave has been determined unreachable
func (this *ElodinaTransportScheduler) SlaveLost(driver scheduler.SchedulerDriver, slaveId *mesos.SlaveID) {
	fmt.Printf("Slave %s has been lost.\n", *slaveId.Value)
}

// mesos.Scheduler interface method.
// Invoked when an executor has exited/terminated.
func (this *ElodinaTransportScheduler) ExecutorLost(scheduler scheduler.SchedulerDriver, executorId *mesos.ExecutorID,
	slaveId *mesos.SlaveID, exitCode int) {
	fmt.Printf("Executor %s on slave %s has exited with %d status code\n", *executorId.Value, *slaveId.Value, exitCode)
}

// mesos.Scheduler interface method.
// Invoked when there is an unrecoverable error in the scheduler or scheduler driver.
func (this *ElodinaTransportScheduler) Error(driver scheduler.SchedulerDriver, err string) {
	fmt.Printf("Scheduler received error: %s\n", err)
}

// Gracefully shuts down all running tasks.
func (this *ElodinaTransportScheduler) Shutdown(driver scheduler.SchedulerDriver) {
	fmt.Println("Shutting down the scheduler.")
}

func (this *ElodinaTransportScheduler) hasEnoughCpuAndMemory(cpusOffered float64, memoryOffered float64) bool {
	return this.config.CpuPerTask <= cpusOffered && this.config.MemPerTask <= memoryOffered
}

func (this *ElodinaTransportScheduler) hasEnoughInstances() bool {
	return this.config.Instances > int(this.runningInstances)
}

func (this *ElodinaTransportScheduler) getRunningInstances() int32 {
	return atomic.LoadInt32(&this.runningInstances)
}

func (this *ElodinaTransportScheduler) incRunningInstances() {
	atomic.AddInt32(&this.runningInstances, 1)
}

func (this *ElodinaTransportScheduler) decRunningInstances() {
	atomic.AddInt32(&this.runningInstances, -1)
}

func (this *ElodinaTransportScheduler) tryKillTask(driver scheduler.SchedulerDriver, taskId *mesos.TaskID) error {
	fmt.Printf("Trying to kill task %s\n", taskId.GetValue())
	var err error
	for i := 0; i <= this.config.KillTaskRetries; i++ {
		if _, err = driver.KillTask(taskId); err == nil {
			return nil
		}
	}
	return err
}

func (this *ElodinaTransportScheduler) removeTask(id *mesos.TaskID) {
	delete(this.taskIdToTaskState, *id.Value)
}

func (this *ElodinaTransportScheduler) takePort(ports *[]*mesos.Value_Range) *uint64 {
	port := (*ports)[0].Begin
	portRange := (*ports)[0]
	portRange.Begin = proto.Uint64((*portRange.Begin) + 1)

	if *portRange.Begin > *portRange.End {
		*ports = (*ports)[1:]
	} else {
		(*ports)[0] = portRange
	}

	return port
}

func (this *ElodinaTransportScheduler) createExecutor(instanceId int32, port uint64) *mesos.ExecutorInfo {
	path := strings.Split(this.config.ExecutorArchiveName, "/")
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("elodina-mirror-%d", instanceId)),
		Name:       proto.String("Elodina Mirror Executor"),
		Source:     proto.String("Elodina"),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --port %d", this.config.ExecutorBinaryName, port)),
			Uris: []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				Value:   proto.String(fmt.Sprintf("http://%s:%d/resource/%s", this.config.ServiceHost, this.config.ServicePort, path[len(path)-1])),
				Extract: proto.Bool(true),
			}},
		},
	}
}

func (this *ElodinaTransportScheduler) subscribeForChanges(group string) {
	events, err := this.coordinator.SubscribeForChanges(group)
	if err != nil {
		panic(err)
	}

	go func() {
		for event := range events {
			if event == kafka.Regular {
				for _, transfer := range this.taskIdToTaskState {
					this.toKill = append(this.toKill, transfer)
				}
			}
		}
	}()
}

func (this *ElodinaTransportScheduler) assignPartitions(transports []*ElodinaTransport) {
	partitionsForTopics, err := this.coordinator.GetPartitionsForTopics(this.config.Topics)
	if err != nil {
		panic(fmt.Sprintf("Failed to obtain partitions for topics: %s, topics: %v", err, this.config.Topics))
	}

	if len(transports) > 0 {
		topicsAndPartitions := make([]*kafka.TopicAndPartition, 0)
		for topic, partitions := range partitionsForTopics {
			for _, partition := range partitions {
				topicsAndPartitions = append(topicsAndPartitions, &kafka.TopicAndPartition{
					Topic:     topic,
					Partition: partition,
				})
			}
		}

		fmt.Printf("%v\n", topicsAndPartitions)

		sort.Sort(hashArray(topicsAndPartitions))
		tasksIterator := circularIterator(&transports)
		for _, topicPartition := range topicsAndPartitions {
			transport := *tasksIterator.Value.(*ElodinaTransport)
			if _, ok := transport.assignment[topicPartition.Topic]; !ok {
				transport.assignment[topicPartition.Topic] = make(map[int]*PartitionState)
			}
			if _, ok := transport.assignment[topicPartition.Topic][int(topicPartition.Partition)]; !ok {
				transport.assignment[topicPartition.Topic][int(topicPartition.Partition)] = NewPartitionState(topicPartition.Topic, int(topicPartition.Partition))
			}

			tasksIterator = tasksIterator.Next()
		}

		for _, transport := range transports {
			this.config.transportConfig.Assignment = make(map[string][]int)
			for topic, partitions := range transport.assignment {
				for partition, _ := range partitions {
					if _, ok := this.config.transportConfig.Assignment[topic]; !ok {
						this.config.transportConfig.Assignment[topic] = make([]int, 0)
					}
					this.config.transportConfig.Assignment[topic] = append(this.config.transportConfig.Assignment[topic], partition)
				}
			}
			configBlob, _ := json.Marshal(this.config.transportConfig)
			transport.task.Executor.Data = configBlob
		}
	}
}
