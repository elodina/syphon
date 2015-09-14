# syphon

### Build instructions
```sh
$ go build scheduler.go
$ go build executor.go
```
 
### Configuration
All Kafka consumer specific configurations are in consumer.properties file.
 
### Run instructions
Once you have everything configured and have credentials(ELODINA_API_KEY and ELODINA_API_USER) for accessing Elodina Log Collection Endpoint, you can run the scheduler
```sh
$ ./scheduler --master zk://${ZK_HOST}:${ZK_PORT}/mesos --topics ${KAFKA_TOPICS} --task.threads 1 --artifacts.host ${ARTIFACT_SERVER_HOST} --artifacts.port ${ARTIFACT_SERVER_PORT} --cpu.per.task 0.1 --mem.per.task 128 --ssl.cert cert.pem --ssl.key key.pem --ssl.cacert server.crt --consumer.config consumer.properties --target.url ${ELODINA_HTTP_ENDPOINT} --api.key ${ELODINA_API_KEY} --api.user ${ELODINA_API_USER} --insecure
```