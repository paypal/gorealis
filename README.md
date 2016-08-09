# gorealis [![GoDoc](https://godoc.org/github.com/rdelval/gorealis?status.svg)](https://godoc.org/github.com/rdelval/gorealis)

Go library for communicating with Apache Aurora.
Named after the northern lights (Aurora Borealis).

### Aurora Compatible version
Please see [AURORA.VER](./AURORA.VER) to see the latest Aurora version against which this
library has been tested against. Vendoring a working version is highly recommended.

## Usage

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

### How to leverage the library (based on the [sample client](examples/client.go))

Create a default configuration file (alternatively, manually create your own Config):
```
config, err := realis.NewDefaultConfig(*url)
```

Create a new Realis struct by passing the configuration struct in:
```
r := realis.NewClient(config)
defer r.Close()
```

Construct a job using an AuroraJob struct.
```
job = realis.NewJob().Environment("prod").
    Role("vagrant").
    Name("hello_world_from_gorealis").
    ExecutorName("docker-compose-executor").
    ExecutorData("{}").
    NumCpus(1).
    Ram(64).
    SetDisk(100).
    IsService(false).
    InstanceCount(1).
    AddPorts(1).
    AddLabel("fileName", "sample-app/sample-app.yml").
    AddURI("https://dl.bintray.com/rdelvalle/mesos-compose-executor/sample-app.tar.gz", true, true)

```

Use client to send a job to Aurora:
```
r.CreateJob(job)
```

Killing an Aurora Job:
```
r.KillJob(job.GetKey())
```

Restarting all instances of an Aurora Job:
```
r.RestartJob(job.GetKey())
```

Adding instances (based on config of instance 0) to Aurora:
```
r.AddInstances(&aurora.InstanceKey{job.GetKey(),0}, 5)
```

Updating the job configuration of a service job:
```
updateJob := realis.NewUpdateJob(job)
updateJob.InstanceCount(1)
updateJob.Ram(128)

msg, err := r.UpdateJob(updateJob, "")
```

## To Do
* Create or import a custom transport that uses https://github.com/jmcvetta/napping to improve efficiency
* Allow library to use ZK to find the master

## Contributions
Contributions are very much welcome. Please raise an issue so that the contribution may be discussed before it's made.
