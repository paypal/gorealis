
# Developing gorealis


### Installing Docker

For our developer environment we leverage of Docker containers.

First you must have Docker installed.  Instructions on how to install Docker
vary from platform to platform and can be found [here](https://docs.docker.com/install/).

### Installing docker-compose

To make the creation of our developer environment as simple as possible, we leverage
docker-compose to bring up all independent components up separately.

This also allows us to delete and recreate our development cluster very quickly.

To install docker-compose please follow the instructions for your platform
[here](https://docs.docker.com/compose/install/).


### Getting the source code

As of go 1.10.x, GOPATH is still relevant. This may change in the future but
for the sake of making development less error prone, it is suggested that the following
directories be created:

`$ mkdir -p $GOPATH/src/github.com/paypal`

And then clone the master branch into the newly created folder:

`$ cd $GOPATH/src/github.com/paypal; git clone git@github.com:paypal/gorealis.git`

Since we check in our vendor folder, gorealis no further set up is needed.

### Bringing up the cluster

To develop gorealis, you will need a fully functioning Mesos cluster along with
Apache Aurora.

In order to bring up our docker-compose set up execute the following command from the root
of the git repository:

`$ docker-compose up -d`

### Testing code

Since Docker does not work well using host mode under MacOS, a workaround has been employed:
docker-compose brings up a bridged network.

* The ports 8081 is exposed for Aurora. http://localhost:8081 will load the Aurora Web UI.
* The port 5050 is exposed for Mesos. http://localhost:5050 will load the Mesos Web UI.

#### Note for developers on MacOS:
Running the cluster using a bridged network on MacOS has some side effects.
Since Aurora exposes it's internal IP location through Zookeeper, gorealis will determine
the address to be 192.168.33.7. The address 192.168.33.7 is valid when running in a Linux
environment but not when running under MacOS. To run code involving the ZK leader fetcher
(such as the tests), a container connected to the network needs to be launched.

For example, running the tests in a container can be done through the following command from
the root of the git repository:

`$ docker run -t -v $(pwd):/go/src/github.com/paypal/gorealis --network gorealis_aurora_cluster golang:1.10.3-alpine  go test github.com/paypal/gorealis`

Or

`$ ./runTestsMac.sh`

Alternatively, if an interactive shell is necessary, the following command may be used:
`$ docker run -it -v $(pwd):/go/src/github.com/paypal/gorealis --network gorealis_aurora_cluster golang:1.10.3-alpine /bin/sh`

### Cleaning up the cluster

If something went wrong while developing and a clean environment is desired, perform the
following command from the root of the git directory:

`$ docker-compose down && docker-compose up -d`


### Tearing down the cluster

Once development is done, the environment may be torn down by executing (from the root of the
git directory):

`$ docker-compose down`



