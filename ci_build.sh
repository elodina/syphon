#!/bin/sh
godep restore && \
go build executor.go && \
go build scheduler.go && \
go build marathon_deploy.go
