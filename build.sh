#!/bin/sh
godep restore

go build scheduler.go
go build executor.go
go build marathon_deploy.go
