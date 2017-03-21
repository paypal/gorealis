/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/rdelval/gorealis"
	"github.com/rdelval/gorealis/gen-go/apache/aurora"
	"github.com/rdelval/gorealis/response"
)

func main() {
	cmd := flag.String("cmd", "", "Job request type to send to Aurora Scheduler")
	executor := flag.String("executor", "thermos", "Executor to use")
	url := flag.String("url", "", "URL at which the Aurora Scheduler exists as [url]:[port]")
	clustersConfig := flag.String("clusters", "", "Location of the clusters.json file used by aurora.")
	clusterName := flag.String("cluster", "devcluster", "Name of cluster to run job on (only necessary if clusters is set)")
	updateId := flag.String("updateId", "", "Update ID to operate on")
	username := flag.String("username", "aurora", "Username to use for authorization")
	password := flag.String("password", "secret", "Password to use for authorization")
	zkUrl := flag.String("zkurl", "", "zookeeper url")
	flag.Parse()

	// Attempt to load leader from zookeeper
	if *clustersConfig != "" {
		clusters, err := realis.LoadClusters(*clustersConfig)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		cluster, ok := clusters[*clusterName]
		if !ok {
			fmt.Printf("Cluster %s chosen doesn't exist\n", *clusterName)
			os.Exit(1)
		}

		*url, err = realis.LeaderFromZK(cluster)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	var job realis.Job
	var err error
	var monitor *realis.Monitor
	var r realis.Realis

	//check if zkUrl is available.
	if *zkUrl != "" {
		fmt.Println("zkUrl: ", *zkUrl)
		cluster := &realis.Cluster{Name: "example",
			AuthMechanism: "UNAUTHENTICATED",
			ZK:            *zkUrl,
			SchedZKPath:   "/aurora/scheduler",
			AgentRunDir:   "latest",
			AgentRoot:     "/var/lib/mesos",
		}
		fmt.Printf("cluster: %+v \n", cluster)

		r, err = realis.NewDefaultClientUsingCluster(cluster, *username, *password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		monitor = &realis.Monitor{r}

	} else {
		r, err = realis.NewDefaultClientUsingUrl(*url, *username, *password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		monitor = &realis.Monitor{r}
	}
	defer r.Close()

	switch *executor {
	case "thermos":
		payload, err := ioutil.ReadFile("examples/thermos_payload.json")
		if err != nil {
			fmt.Println("Error reading json config file: ", err)
			os.Exit(1)
		}

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
		break
	case "compose":
		job = realis.NewJob().
			Environment("prod").
			Role("vagrant").
			Name("docker-compose").
			ExecutorName("docker-compose-executor").
			ExecutorData("{}").
			CPU(0.25).
			RAM(64).
			Disk(100).
			IsService(true).
			InstanceCount(2).
			AddPorts(4).
			AddLabel("fileName", "sample-app/docker-compose.yml").
			AddURIs(true, true, "https://github.com/mesos/docker-compose-executor/releases/download/0.1.0/sample-app.tar.gz")
		break
	case "none":
		job = realis.NewJob().
			Environment("prod").
			Role("vagrant").
			Name("docker_as_task").
			CPU(1).
			RAM(64).
			Disk(100).
			IsService(true).
			InstanceCount(1).
			AddPorts(1)
		break
	default:
		fmt.Println("Only thermos, compose, and none are supported for now")
		os.Exit(1)
	}

	switch *cmd {
	case "create":
		fmt.Println("Creating job")
		resp, err := r.CreateJob(job)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())

		if resp.ResponseCode == aurora.ResponseCode_OK {
			if ok, err := monitor.Instances(job.JobKey(), job.GetInstanceCount(), 5, 50); !ok || err != nil {
				_, err := r.KillJob(job.JobKey())
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				fmt.Println("ok: ", ok)
				fmt.Println("err: ", err)
			}

		}
		break
	case "createDocker":
		fmt.Println("Creating a docker based job")
		container := realis.NewDockerContainer().Image("python:2.7").AddParameter("network", "host")
		job.Container(container)
		resp, err := r.CreateJob(job)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())

		if resp.ResponseCode == aurora.ResponseCode_OK {
			if ok, err := monitor.Instances(job.JobKey(), job.GetInstanceCount(), 10, 300); !ok || err != nil {
				_, err := r.KillJob(job.JobKey())
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
		}
		break
	case "createMesosContainer":
		fmt.Println("Creating a docker based job")
		container := realis.NewMesosContainer().DockerImage("python", "2.7")
		job.Container(container)
		resp, err := r.CreateJob(job)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())

		if resp.ResponseCode == aurora.ResponseCode_OK {
			if ok, err := monitor.Instances(job.JobKey(), job.GetInstanceCount(), 10, 300); !ok || err != nil {
				_, err := r.KillJob(job.JobKey())
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
		}
		break
	case "scheduleCron":
		fmt.Println("Scheduling a Cron job")
		// Cron config
		job.CronSchedule("* * * * *")
		job.IsService(false)
		resp, err := r.ScheduleCronJob(job)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())

		break
	case "startCron":
		fmt.Println("Starting a Cron job")
		resp, err := r.StartCronJob(job.JobKey())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())

		break
	case "descheduleCron":
		fmt.Println("Descheduling a Cron job")
		resp, err := r.DescheduleCronJob(job.JobKey())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())

		break
	case "kill":
		fmt.Println("Killing job")

		resp, err := r.KillJob(job.JobKey())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if resp.ResponseCode == aurora.ResponseCode_OK {
			if ok, err := monitor.Instances(job.JobKey(), 0, 5, 50); !ok || err != nil {
				fmt.Println("Unable to kill all instances of job")
				os.Exit(1)
			}
		}
		fmt.Println(resp.String())
		break
	case "restart":
		fmt.Println("Restarting job")
		resp, err := r.RestartJob(job.JobKey())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(resp.String())
		break
	case "liveCount":
		fmt.Println("Getting instance count")

		live, err := r.GetInstanceIds(job.JobKey(), aurora.LIVE_STATES)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Printf("Live instances: %+v\n", live)
		break
	case "activeCount":
		fmt.Println("Getting instance count")

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println("Number of live instances: ", len(live))
		break
	case "flexUp":
		fmt.Println("Flexing up job")

		numOfInstances := int32(4)

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		currInstances := int32(len(live))
		fmt.Println("Current num of instances: ", currInstances)
		resp, err := r.AddInstances(aurora.InstanceKey{job.JobKey(), 0}, numOfInstances)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if resp.ResponseCode == aurora.ResponseCode_OK {
			if ok, err := monitor.Instances(job.JobKey(), currInstances+numOfInstances, 5, 50); !ok || err != nil {
				fmt.Println("Flexing up failed")
			}
		}
		fmt.Println(resp.String())
		break

	case "flexDown":
		fmt.Println("Flexing down job")

		numOfInstances := int32(2)

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		currInstances := int32(len(live))
		fmt.Println("Current num of instances: ", currInstances)

		resp, err := r.RemoveInstances(job.JobKey(), numOfInstances)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if resp.ResponseCode == aurora.ResponseCode_OK {
			if ok, err := monitor.Instances(job.JobKey(), currInstances-numOfInstances, 5, 50); !ok || err != nil {
				fmt.Println("flexDown failed")
			}
		}
		fmt.Println(resp.String())
		break
	case "update":
		fmt.Println("Updating a job with with more RAM and to 5 instances")
		taskConfig, err := r.FetchTaskConfig(aurora.InstanceKey{job.JobKey(), 0})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		updateJob := realis.NewDefaultUpdateJob(taskConfig)
		updateJob.InstanceCount(5).RAM(128)

		resp, err := r.StartJobUpdate(updateJob, "")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		jobUpdateKey := response.JobUpdateKey(resp)
		monitor.JobUpdate(*jobUpdateKey, 5, 500)

		break
	case "updateDetails":
		resp, err := r.JobUpdateDetails(aurora.JobUpdateQuery{
			Key: &aurora.JobUpdateKey{job.JobKey(), *updateId}, Limit: 1})

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(response.JobUpdateDetails(resp))
		break
	case "abortUpdate":
		fmt.Println("Abort update")
		resp, err := r.AbortJobUpdate(aurora.JobUpdateKey{job.JobKey(), *updateId}, "")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())
		break
	case "rollbackUpdate":
		fmt.Println("Abort update")
		resp, err := r.RollbackJobUpdate(aurora.JobUpdateKey{job.JobKey(), *updateId}, "")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(resp.String())
		break
	case "taskConfig":
		fmt.Println("Getting job info")
		config, err := r.FetchTaskConfig(aurora.InstanceKey{job.JobKey(), 0})

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		print(config.String())
		break

	case "updatesummary":

		fmt.Println("Getting job update summary")
		jobquery := &aurora.JobUpdateQuery{
			Role:   &job.JobKey().Role,
			JobKey: job.JobKey(),
		}
		updatesummary, err := r.GetJobUpdateSummaries(jobquery)
		if err != nil {
			fmt.Printf("error while getting update summary: %v", err)
			os.Exit(1)
		}
		fmt.Println(updatesummary)

	case "taskStatus":
		fmt.Println("Getting task status")
		taskQ := &aurora.TaskQuery{Role: job.JobKey().Role,
			Environment: job.JobKey().Environment,
			JobName:     job.JobKey().Name,
		}
		tasks, err := r.GetTaskStatus(taskQ)
		if err != nil {
			fmt.Printf("error: %+v\n ", err)
			os.Exit(1)
		}
		fmt.Printf("length: %d\n ", len(tasks))
		fmt.Printf("tasks: %+v\n", tasks)

	default:
		fmt.Println("Command not supported")
		os.Exit(1)
	}
}
