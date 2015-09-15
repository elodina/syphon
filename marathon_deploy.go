package main

import (
    "github.com/gambol99/go-marathon"
    "os"
    "log"
    "flag"
    "fmt"
    "time"
)

var master = flag.String("master", "127.0.0.1:5050", "Mesos Master address <ip:port>.")
var topics = flag.String("topics", "", "Comma-separated list of topics")
var threadsPerTask = flag.Int("task.threads", 3, "Max threads per task.")
var artifactServerHost = flag.String("artifacts.host", "0.0.0.0", "Host for artifact server.")
var artifactServerPort = flag.Int("artifacts.port", 8888, "Binding port for artifact server.")
var targetUrl = flag.String("target.url", "", "Target URL.")
var consumerConfigPath = flag.String("consumer.config", "consumer.properties", "Kafka consumer config file")
var certFile = flag.String("ssl.cert", "", "SSL certificate file path.")
var keyFile = flag.String("ssl.key", "", "SSL private key file path.")
var caFile = flag.String("ssl.cacert", "", "Certifying Authority SSL Certificate file path.")
var apiKey = flag.String("api.key", "", "Elodina API key")
var apiUser = flag.String("api.user", "", "Elodina API user")
var marathonURL = flag.String("marathon.url", "http://127.0.0.1:8080", "Marathon URL.")

const launchPattern = "./scheduler --master %s --topics %s --task.threads %d --artifacts.host %s --artifacts.port %d --ssl.cert %s --ssl.key %s --ssl.cacert %s --consumer.config %s --target.url %s --api.key %s --api.user %s --cpu.per.task 0.1 --mem.per.task 128"

func main() {
    flag.Parse()
    marathonConfig := marathon.NewDefaultConfig()
    marathonConfig.URL = *marathonURL
    marathonConfig.LogOutput = os.Stdout
    if marathonClient, err := marathon.NewClient(marathonConfig); err != nil {
        log.Fatal(err)
    } else {
        launchCommand := fmt.Sprintf(launchPattern, *master, *topics, *threadsPerTask, *artifactServerHost, *artifactServerPort, *certFile, *keyFile, *caFile, *consumerConfigPath, *targetUrl, *apiKey, *apiUser)
        healthCheck := marathon.NewDefaultHealthCheck()
        healthCheck.Path = "/health"
        application := &marathon.Application{
            ID:           "/syphon",
            Cmd:          launchCommand,
            CPUs:         1,
            Instances:    1,
            Mem:          256,
            Ports:        []int{*artifactServerPort},
            RequirePorts: true,
            HealthChecks: []*marathon.HealthCheck{healthCheck},
            Container: &marathon.Container{
                Type: "DOCKER",
                Docker: &marathon.Docker{
                    Image: "elodina/syphon",
                    Network: "BRIDGE",
                    PortMappings: []*marathon.PortMapping{
                        &marathon.PortMapping{
                            ContainerPort: *artifactServerPort,
                            HostPort: *artifactServerPort,
                            Protocol: "tcp",
                            ServicePort: *artifactServerPort,
                        },
                    },
                },
            },
        }

        if app, err := marathonClient.CreateApplication(application, false); err != nil {
            fmt.Println(err.Error())
        } else {
            tasks, err := marathonClient.Tasks(fmt.Sprintf("%s", app.ID))
            for err != nil || len(tasks.Tasks) == 0 ||
            !tasks.Tasks[0].HasHealthCheckResults() ||
            !tasks.Tasks[0].HealthCheckResult[len(tasks.Tasks[0].HealthCheckResult)-1].Alive {
                tasks, err = marathonClient.Tasks(fmt.Sprintf("%s", app.ID))
                time.Sleep(5 * time.Second)
            }
            fmt.Println("Successfully deployed")
        }
    }
}