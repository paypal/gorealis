# Running custom executors on Aurora

In this instance we will be using the docker-compose executor to demonstrate
how Aurora can use multiple executors on a single Scheduler.

## Configuring Scheduler to use Docker-Compose executor
In order use the docker compose executor with Aurora, we must first give the scheduler
a configuration file that contains information on how to run the executor.

### Configuration file
The configuration is a JSON file that contains where to find the executor and how to run it.

A sample config file for the docker-compose executor looks like this:
```
[  
  {  
    "executor":{  
      "command":{  
        "value":"java -jar docker-compose-executor_0.0.1.jar",
        "shell":"true",
        "uris":[  
          {  
            "cache":false,
            "executable":true,
            "extract":false,
            "value":"https://dl.bintray.com/rdelvalle/mesos-compose-executor/docker-compose-executor_0.0.1.jar"
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


### Configuring Scheduler to run custom executor
#### Setting the proper flags `-custom_executor_config` flag and enabling mesos fetcher on jobs

The `--custom_executor_config` flag must point to the location of the JSON blob.

The `--enable_mesos_fetcher` flag must be set to true in order to allow jobs to fetch resources.

#### On vagrant
Modify the file `/etc/init/aurora-scheduler.conf` to include:
```
   --custom_executor_config=<Location of JSON blob> \
   --enable_mesos_fetcher=true
```

#### On a scheduler installed via package manager
Modify the file `/etc/default/aurora-scheduler.conf` to include:
```
  AURORA_EXTRA_ARGS="--custom_executor_config=<Location of JSON blob> \
   --enable_mesos_fetcher=true"
```

## Using a custom client
Pystachio does yet support launching tasks using a custom executors. Therefore, a custom
client must be used in order to launch tasks using a custom executor. In this case,
we will be using [gorealis](https://github.com/rdelval/gorealis) to launch a task with
the compose executor on Aurora.

# Configuring the system to run the custom client and docker-compose executor

## Installing Go

### Linux

#### Ubuntu

##### Add PPA and install via apt-get
```
$ sudo add-apt-repository ppa:ubuntu-lxc/lxd-stable
$ sudo apt-get update
$ sudo apt-get install golang
```

##### Set GOPATH

Configure the environment to be able to compile and run Go code.

```
$ mkdir $HOME/go
$ echo export GOPATH=$HOME/go >> $HOME/.profile
$ echo export PATH=$PATH:$GOPATH/bin >> $HOME/.profile
$ echo export PATH=$PATH:$GOROOT/bin >> $HOME/.profile
```

### OS X

One way to install go on OS X is by using [Homebrew](http://brew.sh/)

#### Install Homebrew
Run the following command from the terminal to install Hombrew:
```
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

#### Install Go using Hombrew

Run the following command from the terminal to install Go:
```
$ brew install go
```

#### Set the GOPATH

Configure the environment to be able to compile and run Go code.

```
$ mkdir $HOME/go
$ echo export GOPATH=$HOME/go >> $HOME/.profile
$ echo export GOROOT=/usr/local/opt/go/libexec >> $HOME/.profile
$ echo export PATH=$PATH:$GOPATH/bin >> $HOME/.profile
$ echo export PATH=$PATH:$GOROOT/bin >> $HOME/.profile
```

### Windows

Download and run the msi installer from https://golang.org/dl/

## Installing Docker Compose

In order to run the docker-compose executor, each agent must have docker-compose
installed on it. 

This can be done using pip:
```
$ sudo pip install docker-compose
```

## Downloading gorealis
Finally, we must get `gorealis` using the `go get` command:

```
go get github.com/rdelval/gorealis
```


# Creating Aurora Jobs

### Creating a thermos job
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

### Creating a docker-compose-executor job
Now that we have a thermos job running, it's time to launch a docker-compose job.

We'll be using the gorealis library sample client to send a create job request
to the scheduler, specifiying that we would like to use the docker-compose executor.

Furthermore, we will be specifying what resources we need to download in order to
successfully run a docker compose job.

The job configuration in the sample client looks like this:
```
job = realis.NewJob().
			Environment("prod").
			Role("vagrant").
			Name("docker-compose").
			ExecutorName("docker-compose-executor").
			ExecutorData("{}").
			CPU(1).
			Ram(64).
			Disk(100).
			IsService(false).
			InstanceCount(1).
			AddPorts(1).
			AddLabel("fileName", "sample-app/sample-app.yml").
			AddURI("https://dl.bintray.com/rdelvalle/mesos-compose-executor/sample-app.tar.gz", true, true)
```

Now we run the client sending the create job command to Aurora:
```
go run $GOPATH/src/github.com/rdelval/gorealis/examples/Client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=create
```

If everything went according to plan, a new job will be shown in the Aurora UI.

We can further investigate inside the Mesos task sandbox.

Inside the sandbox, under the sample-app folder, we can find a docker-compose.yml-generated.yml.

If we inspect this file, we can find the port at which we can find the web server we launched.

Under Web->Ports, we find the port Mesos allocated. We can then navigate to:
`<agent address>:<assigned port>`

And a message from the executor should greet us.

## Cleaning up

### Using the Default Client

```
$ aurora job killall devcluster/www-data/prod/hello
$ aurora job killall devcluster/vagrant/prod/docker-compose
```


### Using gorealis

```
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/Client.go -executor=compose -url=http://192.168.33.7:8081 -cmd=kill
$ go run $GOPATH/src/github.com/rdelval/gorealis/examples/Client.go -executor=thermos -url=http://192.168.33.7:8081 -cmd=kill
```



