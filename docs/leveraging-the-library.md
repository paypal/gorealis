# How to leverage the library (based on the [sample client](../examples/client.go))

For a more complete look at the API, please visit https://godoc.org/github.com/paypal/gorealis

* Create a default configuration file (alternatively, manually create your own Config):
```
config, err := realis.NewDefaultConfig(*url)
```

* Create a new Realis client by passing the configuration struct in:
```
r := realis.NewClient(config)
defer r.Close()
```

* Construct a job using a Job struct:
```
job = realis.NewJob().
    Environment("prod").
    Role("vagrant").
    Name("docker-compose").
    ExecutorName("docker-compose-executor").
    ExecutorData("{}").
    CPU(1).
    RAM(64).
    Disk(100).
    IsService(false).
    InstanceCount(1).
    AddPorts(1).
    AddLabel("fileName", "sample-app/docker-compose.yml").
    AddURIs(true, true, "https://github.com/mesos/docker-compose-executor/releases/download/0.1.0/sample-app.tar.gz")
```

* Use client to send a job to Aurora:
```
r.CreateJob(job)
```

* Killing an Aurora Job:
```
r.KillJob(job.GetKey())
```

* Restarting all instances of an Aurora Job:
```
r.RestartJob(job.GetKey())
```

* Adding instances (based on config of instance 0) to Aurora:
```
r.AddInstances(&aurora.InstanceKey{job.GetKey(),0}, 5)
```

* Updating the job configuration of a service job:
```
updateJob := realis.NewUpdateJob(job)
updateJob.InstanceCount(1)
updateJob.Ram(128)
msg, err := r.UpdateJob(updateJob, "")
```