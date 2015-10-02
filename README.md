# syphon

[![Build Status](https://travis-ci.org/elodina/syphon.svg?branch=master)](https://travis-ci.org/elodina/syphon)

### Prerequisites
In order to build this project you need to have [Godep](https://github.com/tools/godep) installed

### Build instructions
```sh
$ ./build.sh
```
 
### Configuration
All Kafka consumer specific configurations are in consumer.properties file.
 
### Scheduler run instructions
Once you have everything configured and have credentials(ELODINA_API_KEY and ELODINA_API_USER) for accessing Elodina Log Collection Endpoint, you can run the scheduler
```sh
$ ./scheduler --master zk://${ZK_HOST}:${ZK_PORT}/mesos --topics ${KAFKA_TOPICS} --task.threads 1 --artifacts.host ${ARTIFACT_SERVER_HOST} --artifacts.port ${ARTIFACT_SERVER_PORT} --cpu.per.task 0.1 --mem.per.task 128 --ssl.cert cert.pem --ssl.key key.pem --ssl.cacert server.crt --consumer.config consumer.properties --target.url ${ELODINA_HTTP_ENDPOINT} --api.key ${ELODINA_API_KEY} --api.user ${ELODINA_API_USER} --insecure
```

### Run in Docker
```sh
$ sudo docker build -t elodina/syphon . 
```  
```sh
$ sudo docker run --net=host -i -t elodina/syphon ./scheduler --master zk://${ZK_HOST}:${ZK_PORT}/mesos --topics ${KAFKA_TOPICS} --task.threads 1 --artifacts.host ${ARTIFACT_SERVER_HOST} --artifacts.port ${ARTIFACT_SERVER_PORT} --cpu.per.task 0.1 --mem.per.task 128 --ssl.cert cert.pem --ssl.key key.pem --ssl.cacert server.crt --consumer.config consumer.properties --target.url ${ELODINA_HTTP_ENDPOINT} --api.key ${ELODINA_API_KEY} --api.user ${ELODINA_API_USER} --insecure 
```
