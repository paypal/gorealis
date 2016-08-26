# Using the Sample client

## Usage: 
```
Usage of client:
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
These commands are set to run on a vagrant box. To be able to runt he docker compose
executor examples, the vagrant box must be configured properly to use the docker compose executor.

### Thermos

#### Creating a Thermos job
```
$ cd $GOPATH/src/github.com/rdelval/gorealis
$ go run examples/client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=create
```
#### Kill a Thermos job
```
$ go run $GOPATH/src/github.com/rdelval/gorealis.git/examples/client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=kill
```

### Docker Compose executor (custom executor)

#### Creating Docker Compose executor job
```
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=create
```
#### Kill a Docker Compose executor job
```
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=kill
```
