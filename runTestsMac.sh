#!/bin/bash

# Since we run our docker compose setup in bridge mode to be able to run on MacOS, we have to launch a Docker container within the bridge network in order to avoid any routing issues.
docker run --rm -t -w /gorealis  -v $GOPATH/pkg:/go/pkg -v $(pwd):/gorealis --network gorealis_aurora_cluster golang:1.16-buster go test -v github.com/paypal/gorealis $@
