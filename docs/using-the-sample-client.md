### Using the Sample client with Thermos
Clone Aurora:
```
$ git clone  git://git.apache.org/aurora.git
```

Bring up the vagrant image:
```
$ cd aurora
$ vagrant up
```

Download and run the Client to create a Thermos Job:
```
$ go get github.com/rdelval/gorealis
$ cd $GOPATH/src/github.com/rdelval/gorealis
$ go run examples/client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=create
```

### Using the Sample client with Docker Compose executor

Clone modified version of Aurora repo and checkout the right branch:
```
$ git clone git@github.com:rdelval/aurora.git
$ git checkout DockerComposeExecutor
```

Bring up the vagrant image:
```
$ cd aurora
$ vagrant up
```

Download and run the Client to create a Docker-Compose Job:
```
$ go get github.com/rdelval/gorealis
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=create
```
From the [Aurora web UI](http://192.168.33.7:8081/scheduler/vagrant/prod/docker-compose/0), create struct dump by clicking on the task ID.
In the struct dump, find the port assigned to the task (named "port0"). 

Navigate to the 192.168.33.7:`<assigned port>`.
If the page is not found, wait a few minutes while the docker image is downloaded and the container is deployed.

Finally, terminate the job:
```
$ go run $GOPATH/src/github.com/rdelval/gorealis.git/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=kill
```
