### How to leverage the library (based on the [sample client](../examples/client.go))

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

