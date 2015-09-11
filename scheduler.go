package main

import (
	"flag"
	"fmt"
	"github.com/elodina/syphon/consumer"
	"github.com/elodina/syphon/framework"
	"github.com/golang/protobuf/proto"
	"github.com/jimlawless/cfg"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var master = flag.String("master", "127.0.0.1:5050", "Mesos Master address <ip:port>.")
var topics = flag.String("topics", "", "Comma-separated list of topics")
var threadsPerTask = flag.Int("task.threads", 3, "Max threads per task.")
var artifactServerHost = flag.String("artifacts.host", "0.0.0.0", "Host for artifact server.")
var artifactServerPort = flag.Int("artifacts.port", 8888, "Binding port for artifact server.")
var cpuPerTask = flag.Float64("cpu.per.task", 0.2, "CPUs per task.")
var memPerTask = flag.Float64("mem.per.task", 256, "Memory per task.")
var targetUrl = flag.String("target.url", "", "Target URL.")
var consumerConfigPath = flag.String("consumer.config", "consumer.properties", "Kafka consumer config file")

func main() {
	flag.Parse()

	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)

	frameworkInfo := &mesosproto.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Syphon Framework"),
	}

	schedulerConfig := framework.NewElodinaTransportSchedulerConfig()
	schedulerConfig.CpuPerTask = *cpuPerTask
	schedulerConfig.MemPerTask = *memPerTask
	schedulerConfig.Topics = strings.Split(*topics, ",")
	schedulerConfig.ExecutorBinaryName = "executor"
	schedulerConfig.ServiceHost = *artifactServerHost
	schedulerConfig.ServicePort = *artifactServerPort
	schedulerConfig.ThreadsPerTask = *threadsPerTask
    schedulerConfig.TargetURL = *targetUrl
	schedulerConfig.ConsumerConfig = mustReadConsumerConfig(*consumerConfigPath)

	transportScheduler := framework.NewElodinaTransportScheduler(schedulerConfig)
	driverConfig := scheduler.DriverConfig{
		Scheduler: transportScheduler,
		Framework: frameworkInfo,
		Master:    *master,
	}

	driver, err := scheduler.NewMesosSchedulerDriver(driverConfig)
	go func() {
		<-ctrlc
		transportScheduler.Shutdown(driver)
		driver.Stop(false)
	}()

	if err != nil {
		fmt.Println("Unable to create a SchedulerDriver ", err.Error())
	}

	go startArtifactServer()

	if stat, err := driver.Run(); err != nil {
		fmt.Println("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
}

func startArtifactServer() {
	http.HandleFunc(fmt.Sprintf("/resource/"), func(w http.ResponseWriter, r *http.Request) {
		resourceTokens := strings.Split(r.URL.Path, "/")
		resource := resourceTokens[len(resourceTokens)-1]
		fmt.Println("Serving ", resource)
		http.ServeFile(w, r, resource)
	})
	http.ListenAndServe(fmt.Sprintf("%s:%d", *artifactServerHost, *artifactServerPort), nil)
}

func mustReadConsumerConfig(path string) consumer.PartitionConsumerConfig {
	config := consumer.PartitionConsumerConfig{}
	cfgMap, err := cfg.LoadNewMap(path)
	if err != nil {
		panic(err)
	}

	config.ClientID = cfgMap["client.id"]
	config.BrokerList = strings.Split(cfgMap["broker.list"], ",")
	commitOffsetBackoff, err := time.ParseDuration(cfgMap["commit.backoff"])
	if err != nil {
		panic(err)
	}
	config.CommitOffsetBackoff = commitOffsetBackoff
	commitOffsetRetries, err := strconv.Atoi(cfgMap["commit.retries"])
	if err != nil {
		panic(err)
	}
	config.CommitOffsetRetries = commitOffsetRetries
	connectTimeout, err := time.ParseDuration(cfgMap["connect.timeout"])
	if err != nil {
		panic(err)
	}
	config.ConnectTimeout = connectTimeout
	consumerMetadataBackoff, err := time.ParseDuration(cfgMap["metadata.backoff"])
	if err != nil {
		panic(err)
	}
	config.ConsumerMetadataBackoff = consumerMetadataBackoff
	consumerMetadataRetries, err := strconv.Atoi(cfgMap["consumer.metadata.retries"])
	if err != nil {
		panic(err)
	}
	config.ConsumerMetadataRetries = consumerMetadataRetries
	fetchMaxWaitTime, err := strconv.Atoi(cfgMap["fetch.max.wait"])
	if err != nil {
		panic(err)
	}
	config.FetchMaxWaitTime = int32(fetchMaxWaitTime)
	fetchMinBytes, err := strconv.Atoi(cfgMap["fetch.min.bytes"])
	if err != nil {
		panic(err)
	}
	config.FetchMinBytes = int32(fetchMinBytes)
	fetchSize, err := strconv.Atoi(cfgMap["fetch.size"])
	if err != nil {
		panic(err)
	}
	config.FetchSize = int32(fetchSize)
	keepAlive, err := strconv.ParseBool(cfgMap["keep.alive"])
	if err != nil {
		panic(err)
	}
	config.KeepAlive = keepAlive
	keepAliveTimeout, err := time.ParseDuration(cfgMap["keep.alive.timeout"])
	if err != nil {
		panic(err)
	}
	config.KeepAliveTimeout = keepAliveTimeout
	maxConnections, err := strconv.Atoi(cfgMap["max.connections"])
	if err != nil {
		panic(err)
	}
	config.MaxConnections = maxConnections
	maxConnectionsPerBroker, err := strconv.Atoi(cfgMap["max.broker.connections"])
	if err != nil {
		panic(err)
	}
	config.MaxConnectionsPerBroker = maxConnectionsPerBroker
	metadataBackoff, err := time.ParseDuration(cfgMap["metadata.backoff"])
	if err != nil {
		panic(err)
	}
	config.MetadataBackoff = metadataBackoff
	metadataRetries, err := strconv.Atoi(cfgMap["metadata.retries"])
	if err != nil {
		panic(err)
	}
	config.MetadataRetries = metadataRetries
	readTimeout, err := time.ParseDuration(cfgMap["read.timeout"])
	if err != nil {
		panic(err)
	}
	config.ReadTimeout = readTimeout
	writeTimeout, err := time.ParseDuration(cfgMap["write.timeout"])
	if err != nil {
		panic(err)
	}
	config.WriteTimeout = writeTimeout

	return config
}
