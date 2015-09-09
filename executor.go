package main
import (
    "fmt"
    "github.com/mesos/mesos-go/executor"
    "github.com/elodina/syphon/framework"
    "flag"
    "github.com/gorilla/mux"
    "log"
    "net/http"
    "encoding/json"
    "github.com/elodina/syphon/consumer"
)

var certFile = flag.String("ssl.cert", "", "SSL certificate file path.")
var keyFile = flag.String("ssl.key", "", "SSL private key file path.")
var caFile = flag.String("ssl.cacert", "", "Certifying Authority SSL Certificate file path.")
var targetUrl = flag.String("target.url", "", "Target URL.")
var port = flag.Int("port", 8989, "Port to bind to")

func main() {
    flag.Parse()
    fmt.Println("Starting Elodina Executor")
    httpMirrorExecutor := framework.NewHttpMirrorExecutor(*certFile, *keyFile, *caFile, *targetUrl)
    driverConfig := executor.DriverConfig{
        Executor: httpMirrorExecutor,
    }
    driver, err := executor.NewMesosExecutorDriver(driverConfig)

    server := ExecutorHTTPServer{httpMirrorExecutor}
    server.Start()

    if err != nil {
        fmt.Println("Unable to create a ExecutorDriver ", err.Error())
    }

    _, err = driver.Start()
    if err != nil {
        fmt.Println("Got error:", err)
        return
    }
    fmt.Println("Executor process has started and running.")
    driver.Join()
}

type ExecutorHTTPServer struct {
    httpMirrorExecutor *framework.HttpMirrorExecutor
}

func (this *ExecutorHTTPServer) Start() {
    r := mux.NewRouter()
    r.HandleFunc("/assign", this.Assign).Methods("POST")

    endpoint := fmt.Sprintf("%s:%d", *port)
    log.Printf("Serving on %s\n", endpoint)

    http.ListenAndServe(endpoint, r)
}

func (this *ExecutorHTTPServer) Assign(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    assignments := make([]consumer.TopicAndPartition, 0)
    err := json.NewDecoder(req.Body).Decode(&assignments)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    this.httpMirrorExecutor.Assign(assignments)
}