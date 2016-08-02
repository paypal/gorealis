# GoRealis

Go library for communicating with Apache Aurora.
Named after the northern lights (Aurora Borealis).

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
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/Client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=create
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
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/Client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=create
```
From the [Aurora web UI](http://192.168.33.7:8081/scheduler/vagrant/prod/docker-compose/0), create struct dump by clicking on the task ID.
In the struct dump, find the port assigned to the task (named "port0"). 

Navigate to the 192.168.33.7:`<assigned port>`. Currently the redis image being deployed is broken, so an error is expected.
If the page is not found, wait a few minutes while the docker image is downloaded and the container is deployed.

Finally, terminate the job:
```
$ go run $GOPATH/src/github.com/rdelval/gorealis.git/examples/Client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=kill
```

### Leveraging the library 
Commands available: create, kill, restart

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
job = realis.NewJob().SetEnvironment("prod").
    SetRole("vagrant").
    SetName("hello_world_from_gorealis").
    SetExecutorName("docker-compose-executor").
    SetExecutorData("{}").
    SetNumCpus(1).
    SetRam(64).
    SetDisk(100).
    SetIsService(false).
    SetInstanceCount(1).
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
r.AddInstances(job.GetKey(), 5)
```

Updating the job configuration of a service job:
```
updateJob := realis.NewUpdateJob(job)
updateJob.SetInstanceCount(1)
updateJob.SetRam(128)

msg, err := r.UpdateJob(updateJob, "")
```

### Methods:

|Method    | Arguments  | Description|
|----------|------------|------------|
|CreateJob | `*Job` | Sends a job create request to Apache Aurora |
|KillJob   | `*aurora.JobKey` | Attempts to kill all active instances running in Aurora. Only needs environment, role, name |
|RestartJob| `*aurora.JobKey` | Attempts to restart all active instances running in Aurora |
|AddInstances|`*aurora.JobKey`, `int32`| Launches the specified number of new instances based on existing job config |
|StartUpdateJob|`*UpdateJob`, `string`| Updates a service job with a new configuration |
|AbortUpdateJob|`*aurora.Jobkey`, `string`, `string`| Abort the job update that matches the ID |

## To Do
* Create or import a custom transport that uses https://github.com/jmcvetta/napping to improve efficiency
* Allow library to use ZK to find the master

## Contributions
Contributions are very much welcome. Please raise an issue so that the contribution may be discussed before it's made.
