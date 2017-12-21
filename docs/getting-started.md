# Running custom executors on Aurora

In this document, we will be using the docker-compose executor to demonstrate
how Aurora can use multiple executors on a single Scheduler. For this guide,
we will be using a vagrant instance to demonstrate the setup process. Many of the same
steps also apply to an Aurora installation made via a package manager. Differences in how to configure
the cluster between the vagrant image and the package manager will be clarified when necessary.

## Configuring Aurora manually
### Spinning up an Aurora instance with Vagrant
Follow the guide at http://aurora.apache.org/documentation/latest/getting-started/vagrant/
until the end of step 4 (Start the local cluster) and skip to configuring Docker-Compose executor.

### Installing Aurora through a package manager
Follow the guide at http://aurora.apache.org/documentation/latest/operations/installation/

### Configuring Scheduler to use Docker-Compose executor
In order to use the docker compose executor with Aurora, we must first give the scheduler
a configuration file that contains information on how to run the executor.

#### Configuration file
The configuration is a JSON file that contains where to find the executor and how to run it.

More information about how an executor may be configured for consumption by Aurora can be found [here](https://github.com/apache/aurora/blob/master/docs/operations/configuration.md#custom-executors)
under the custom executors section.

A sample config file for the docker-compose executor looks like this:
```
[  
  {  
    "executor":{  
      "command":{  
        "value":"java -jar docker-compose-executor_0.1.0.jar",
        "shell":"true",
        "uris":[  
          {  
            "cache":false,
            "executable":true,
            "extract":false,
            "value":"https://github.com/mesos/docker-compose-executor/releases/download/0.1.0/docker-compose-executor_0.1.0.jar"
          }
        ]
      },
      "name":"docker-compose-executor",
      "resources":[]
    },
    "task_prefix":"compose-"
  }
]

```


#### Configuring the Scheduler to run a custom executor
##### Setting the proper flags
Some flags need to be set on the Aurora scheduler in order for custom executors to work properly.

The `-custom_executor_config` flag must point to the location of the JSON blob.

The `-enable_mesos_fetcher` flag must be set to true in order to allow jobs to fetch resources.

##### On vagrant
* Log into the vagrant image by going to the folder at which the Aurora repository
was cloned and running:
```
$ vagrant ssh
```

* Write the sample JSON blob provided above to a file inside the vagrant image.

* Inside the vagrant image, modify the file `/etc/init/aurora-scheduler.conf` to include:
```
   -custom_executor_config=<Location of JSON blob> \
   -enable_mesos_fetcher=true
```

##### On a scheduler installed via package manager
* Write the sample JSON blob provided above to a file on the same machine where the scheduler is running.

* Modify `EXTRA_SCHEDULER_ARGS` in the file file `/etc/default/aurora-scheduler` to be:
```
  EXTRA_SCHEDULER_ARGS="-custom_executor_config=<Location of JSON blob> -enable_mesos_fetcher=true"
```

For these configurations to kick in, the aurora-scheduler must be restarted.
Depending on the distribution of choice being used this command may look a bit different.

On Ubuntu, restarting the aurora-scheduler can be achieved by running the following command:
```
$ sudo service aurora-scheduler restart
```

### Using a custom client
Pystachio does not yet support launching tasks using custom executors. Therefore, a custom
client must be used in order to launch tasks using a custom executor. In this case,
we will be using [gorealis](https://github.com/paypal/gorealis) to launch a task with
the compose executor on Aurora.

## Configuring the system to run a custom client and docker-compose executor

### Installing Go

#### Linux

##### Ubuntu

###### Adding a PPA and install via apt-get
```
$ sudo add-apt-repository ppa:ubuntu-lxc/lxd-stable
$ sudo apt-get update
$ sudo apt-get install golang
```

###### Configuring the GOPATH

Configure the environment to be able to compile and run Go code.
```
$ mkdir $HOME/go
$ echo export GOPATH=$HOME/go >> $HOME/.bashrc
$ echo export GOROOT=/usr/lib/go >> $HOME/.bashrc
$ echo export PATH=$PATH:$GOPATH/bin >> $HOME/.bashrc
$ echo export PATH=$PATH:$GOROOT/bin >> $HOME/.bashrc
```

Finally we must reload the .bashrc configuration:
```
$ source $HOME/.bashrc
```

#### OS X

One way to install go on OS X is by using [Homebrew](http://brew.sh/)

##### Installing Homebrew
Run the following command from the terminal to install Hombrew:
```
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

##### Installing Go using Hombrew

Run the following command from the terminal to install Go:
```
$ brew install go
```

##### Configuring the GOPATH

Configure the environment to be able to compile and run Go code.
```
$ mkdir $HOME/go
$ echo export GOPATH=$HOME/go >> $HOME/.profile
$ echo export GOROOT=/usr/local/opt/go/libexec >> $HOME/.profile
$ echo export PATH=$PATH:$GOPATH/bin >> $HOME/.profile
$ echo export PATH=$PATH:$GOROOT/bin >> $HOME/.profile
```

Finally we must reload the .profile configuration:
```
$ source $HOME/.profile
```

#### Windows

Download and run the msi installer from https://golang.org/dl/

## Installing Docker Compose
To show Aurora's new multi executor feature, we need to use at least one custom executor.
In this case we will be using the [docker-compose-executor](https://github.com/mesos/docker-compose-executor).

In order to run the docker-compose executor, each agent must have docker-compose installed on it.

This can be done using pip:
```
$ sudo pip install docker-compose
```

## Downloading gorealis
Finally, we must get `gorealis` using the `go get` command:

```
go get github.com/paypal/gorealis
```

# Creating Aurora Jobs

## Creating a thermos job
To demonstrate that we are able to run jobs using different executors on the 
same scheduler, we'll first launch a thermos job using the default Aurora Client.

We can use a sample job for this:

hello_world.aurora
```
hello = Process(
  name = 'hello',
  cmdline = """
    while true; do
      echo hello world
      sleep 10
    done
  """)

task = SequentialTask(
  processes = [hello],
  resources = Resources(cpu = 1.0, ram = 128*MB, disk = 128*MB))

jobs = [Service(
  task = task, cluster = 'devcluster', role = 'www-data', environment = 'prod', name = 'hello')]
```

Now we create the job:
```
aurora job create devcluster/www-data/prod/hello hello_world.aurora

```

Note that user `www-data` must exist on the Agent on which the task will be run.
If that user doesn't exist, please modify the role value inside the .aurora file as well
as the command to a user that exists on the machine on which the task will be run.

## Creating a docker-compose-executor job
Now that we have a thermos job running, it's time to launch a docker-compose job.

We'll be using the gorealis library sample client to send a create job request
to the scheduler, specifying that we would like to use the docker-compose executor.

Furthermore, we will be specifying what resources we need to download in order to
successfully run a docker compose job.

For example, the job configuration in the sample client looks like this:
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

Using a vagrant setup as an example, we can run the following command to create a compose job:
```
go run $GOPATH/src/github.com/paypal/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=create
```

If everything went according to plan, a new job will be shown in the Aurora UI.
We can further investigate inside the Mesos task sandbox. Inside the sandbox, under 
the sample-app folder, we can find a docker-compose.yml-generated.yml. If we inspect this file, 
we can find the port at which we can find the web server we launched.

Under Web->Ports, we find the port Mesos allocated. We can then navigate to:
`<agent address>:<assigned port>`. (In vagrant's case the agent address is `192.68.33.7`)

A message from the executor should greet us.

## Creating a Thermos job using gorealis
It is also possible to create a thermos job using gorealis. To do this, however, 
a thermos payload is required. A thermos payload consists of a JSON blob that details
the entire task as it exists inside the Aurora Scheduler. *Creating the blob is unfortunately
out of the scope of was gorealis does*, so a thermos payload must be generated beforehand or 
retrieved from the structdump of an existing task for testing purposes.

A sample thermos JSON payload may be found [here](../examples/thermos_payload.json) in the examples folder.

The job struct configuration for a Thermos job looks something like this:
```
payload, err := ioutil.ReadFile("examples/thermos_payload.json")

job = realis.NewJob().
    Environment("prod").
    Role("vagrant").
    Name("hello_world_from_gorealis").
    ExecutorName(aurora.AURORA_EXECUTOR_NAME).
    ExecutorData(string(payload)).
    CPU(1).
    RAM(64).
    Disk(100).
    IsService(true).
    InstanceCount(1).
    AddPorts(1)
```

Using a vagrant setup as an example, we can run the following command to create a Thermos job:
```
$ cd $GOPATH/src/github.com/paypal/gorealis
$ go run examples/client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=create -executor=thermos
```

# Cleaning up

To stop the jobs we've launched, we need to send a job kill request to Aurora.
It should be noted that although we can't create jobs using a custom executor using the default Aurora client,
we ~can~ use the default Aurora client to kill them. Additionally, we can use gorealis perform the clean up as well.

## Using the Default Client

```
$ aurora job killall devcluster/www-data/prod/hello
$ aurora job killall devcluster/vagrant/prod/docker-compose
```

## Using gorealis

```
$ go run $GOPATH/src/github.com/paypal/gorealis/examples/client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=kill
$ go run $GOPATH/src/github.com/paypal/gorealis/examples/client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=kill
```
