# Using the Sample client

## Usage: 
```
Usage of ./client:
  -cluster string
        Name of cluster to run job on (default "devcluster")
  -clusters string
        Location of the clusters.json file used by aurora.
  -cmd string
        Job request type to send to Aurora Scheduler
  -executor string
        Executor to use (default "thermos")
  -password string
        Password to use for authorization (default "secret")
  -updateId string
        Update ID to operate on
  -url string
        URL at which the Aurora Scheduler exists as [url]:[port]
  -username string
        Username to use for authorization (default "aurora")
```

## Sample commands:
These commands are set to run on a vagrant box. To be able to run the docker compose
executor examples, the vagrant box must be configured properly to use the docker compose executor.

### Thermos

#### Creating a Thermos job
```
$ cd $GOPATH/src/github.com/paypal/gorealis/examples
$ go run client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=create
```
#### Kill a Thermos job
```
$ go run $GOPATH/src/github.com/paypal/gorealis/examples/client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=kill
```

### Docker Compose executor (custom executor)

#### Creating Docker Compose executor job
```
$ go run $GOPATH/src/github.com/paypal/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=create
```
#### Kill a Docker Compose executor job
```
$ go run $GOPATH/src/github.com/paypal/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=kill
```
