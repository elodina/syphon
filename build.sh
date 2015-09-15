#!/bin/sh
gvp init
gvp in
gpm install

go build scheduler.go
go build executor.go
go build marathon_deploy.go
