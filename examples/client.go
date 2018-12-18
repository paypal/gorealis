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
	"log"
	"strings"
	"time"

	realis "github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

var cmd, executor, url, clustersConfig, clusterName, updateId, username, password, zkUrl, hostList, role string
var caCertsPath string
var clientKey, clientCert string

var ConnectionTimeout = 20 * time.Second

func init() {
	flag.StringVar(&cmd, "cmd", "", "Aurora Job request type to send to Aurora Scheduler")
	flag.StringVar(&executor, "executor", "thermos", "Executor to use")
	flag.StringVar(&url, "url", "", "URL at which the Aurora Scheduler exists as [url]:[port]")
	flag.StringVar(&clustersConfig, "clusters", "", "Location of the clusters.json file used by aurora.")
	flag.StringVar(&clusterName, "cluster", "devcluster", "Name of cluster to run job on (only necessary if clusters is set)")
	flag.StringVar(&updateId, "updateId", "", "Update ID to operate on")
	flag.StringVar(&username, "username", "aurora", "Username to use for authorization")
	flag.StringVar(&password, "password", "secret", "Password to use for authorization")
	flag.StringVar(&zkUrl, "zkurl", "", "zookeeper url")
	flag.StringVar(&hostList, "hostList", "", "Comma separated list of hosts to operate on")
	flag.StringVar(&role, "role", "", "owner role to use")
	flag.StringVar(&caCertsPath, "caCertsPath", "", "Path to CA certs on local machine.")
	flag.StringVar(&clientCert, "clientCert", "", "Client certificate to use to connect to Aurora.")
	flag.StringVar(&clientKey, "clientKey", "", "Client private key to use to connect to Aurora.")

	flag.Parse()

	// Attempt to load leader from zookeeper using a
	// cluster.json file used for the default aurora client if provided.
	// This will override the provided url in the arguments
	if clustersConfig != "" {
		clusters, err := realis.LoadClusters(clustersConfig)
		if err != nil {
			log.Fatalln(err)
		}

		cluster, ok := clusters[clusterName]
		if !ok {
			log.Fatalf("Cluster %s doesn't exist in the file provided\n", clusterName)
		}

		url, err = realis.LeaderFromZK(cluster)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func main() {

	var job *realis.AuroraJob
	var err error
	var r *realis.Client

	clientOptions := []realis.ClientOption{
		realis.BasicAuth(username, password),
		realis.ThriftJSON(),
		realis.Timeout(ConnectionTimeout),
		realis.BackOff(realis.Backoff{
			Steps:    2,
			Duration: 10 * time.Second,
			Factor:   2.0,
			Jitter:   0.1,
		}),
		realis.Debug(),
	}

	// Check if zkUrl is available.
	if zkUrl != "" {
		fmt.Println("zkUrl: ", zkUrl)
		clientOptions = append(clientOptions, realis.ZKUrl(zkUrl))
	} else {
		clientOptions = append(clientOptions, realis.SchedulerUrl(url))
	}

	if caCertsPath != "" {
		clientOptions = append(clientOptions, realis.CertsPath(caCertsPath))
	}

	if clientKey != "" && clientCert != "" {
		clientOptions = append(clientOptions, realis.ClientCerts(clientKey, clientCert))
	}

	r, err = realis.NewClient(clientOptions...)
	if err != nil {
		log.Fatalln(err)
	}
	defer r.Close()

	switch executor {
	case "thermos":
		payload, err := ioutil.ReadFile("examples/thermos_payload.json")
		if err != nil {
			log.Fatalln("Error reading json config file: ", err)
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
	case "compose":
		job = realis.NewJob().
			Environment("prod").
			Role("vagrant").
			Name("docker-compose-test").
			ExecutorName("docker-compose-executor").
			ExecutorData("{}").
			CPU(0.25).
			RAM(512).
			Disk(100).
			IsService(true).
			InstanceCount(1).
			AddPorts(4).
			AddLabel("fileName", "sample-app/docker-compose.yml").
			AddURIs(true, true, "https://github.com/mesos/docker-compose-executor/releases/download/0.1.0/sample-app.tar.gz")
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
	default:
		log.Fatalln("Only thermos, compose, and none are supported for now")
	}

	switch cmd {
	case "create":
		fmt.Println("Creating job")
		err := r.CreateJob(job)
		if err != nil {
			log.Fatalln(err)
		}

		if ok, mErr := r.InstancesMonitor(job.JobKey(), job.GetInstanceCount(), 5*time.Second, 50*time.Second); !ok || mErr != nil {
			err := r.KillJob(job.JobKey())
			if err != nil {
				log.Fatalln(err)
			}
			log.Fatalf("ok: %v\n err: %v", ok, mErr)
		}

	case "createService":
		// Create a service with three instances using the update API instead of the createJob API
		fmt.Println("Creating service")
		settings := realis.JobUpdateFromConfig(job.TaskConfig()).InstanceCount(3)
		result, err := r.CreateService(settings)
		if err != nil {
			log.Fatal("error: ", err)
		}
		fmt.Println(result.String())

		if ok, mErr := r.JobUpdateMonitor(*result.GetKey(), 5*time.Second, 180*time.Second); !ok || mErr != nil {
			err := r.AbortJobUpdate(*result.GetKey(), "Monitor timed out")
			err = r.KillJob(job.JobKey())
			if err != nil {
				log.Fatal(err)
			}
			log.Fatalf("ok: %v\n err: %v", ok, mErr)
		}

	case "createDocker":
		fmt.Println("Creating a docker based job")
		container := realis.NewDockerContainer().Image("python:2.7").AddParameter("network", "host")
		job.Container(container)
		err := r.CreateJob(job)
		if err != nil {
			log.Fatal(err)
		}

		if ok, err := r.InstancesMonitor(job.JobKey(), job.GetInstanceCount(), 10*time.Second, 300*time.Second); !ok || err != nil {
			err := r.KillJob(job.JobKey())
			if err != nil {
				log.Fatal(err)
			}
		}

	case "createMesosContainer":
		fmt.Println("Creating a docker based job")
		container := realis.NewMesosContainer().DockerImage("python", "2.7")
		job.Container(container)
		err := r.CreateJob(job)
		if err != nil {
			log.Fatal(err)
		}

		if ok, err := r.InstancesMonitor(job.JobKey(), job.GetInstanceCount(), 10*time.Second, 300*time.Second); !ok || err != nil {
			err := r.KillJob(job.JobKey())
			if err != nil {
				log.Fatal(err)
			}
		}

	case "scheduleCron":
		fmt.Println("Scheduling a Cron job")
		// Cron config
		job.CronSchedule("* * * * *")
		job.IsService(false)
		err := r.ScheduleCronJob(job)
		if err != nil {
			log.Fatal(err)
		}

	case "startCron":
		fmt.Println("Starting a Cron job")
		err := r.StartCronJob(job.JobKey())
		if err != nil {
			log.Fatal(err)
		}

	case "descheduleCron":
		fmt.Println("Descheduling a Cron job")
		err := r.DescheduleCronJob(job.JobKey())
		if err != nil {
			log.Fatal(err)
		}

	case "kill":
		fmt.Println("Killing job")

		err := r.KillJob(job.JobKey())
		if err != nil {
			log.Fatal(err)
		}

		if ok, err := r.InstancesMonitor(job.JobKey(), 0, 5*time.Second, 50*time.Second); !ok || err != nil {
			log.Fatal("Unable to kill all instances of job")
		}

	case "restart":
		fmt.Println("Restarting job")
		err := r.RestartJob(job.JobKey())
		if err != nil {
			log.Fatal(err)
		}

	case "liveCount":
		fmt.Println("Getting instance count")

		live, err := r.GetInstanceIds(job.JobKey(), aurora.LIVE_STATES)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Live instances: %+v\n", live)

	case "activeCount":
		fmt.Println("Getting instance count")

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Active instances: ", live)

	case "flexUp":
		fmt.Println("Flexing up job")

		numOfInstances := 4

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			log.Fatal(err)
		}
		currInstances := len(live)
		fmt.Println("Current num of instances: ", currInstances)

		key := job.JobKey()
		err = r.AddInstances(aurora.InstanceKey{
			JobKey:     &key,
			InstanceId: live[0],
		},
			int32(numOfInstances))

		if err != nil {
			log.Fatal(err)
		}

		if ok, err := r.InstancesMonitor(job.JobKey(), int32(currInstances+numOfInstances), 5*time.Second, 50*time.Second); !ok || err != nil {
			fmt.Println("Flexing up failed")
		}

	case "flexDown":
		fmt.Println("Flexing down job")

		numOfInstances := 2

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			log.Fatal(err)
		}
		currInstances := len(live)
		fmt.Println("Current num of instances: ", currInstances)

		err = r.RemoveInstances(job.JobKey(), numOfInstances)
		if err != nil {
			log.Fatal(err)
		}

		if ok, err := r.InstancesMonitor(job.JobKey(), int32(currInstances-numOfInstances), 5*time.Second, 100*time.Second); !ok || err != nil {
			fmt.Println("flexDown failed")
		}

	case "update":
		fmt.Println("Updating a job with with more RAM and to 5 instances")
		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			log.Fatal(err)
		}

		key := job.JobKey()
		taskConfig, err := r.FetchTaskConfig(aurora.InstanceKey{
			JobKey:     &key,
			InstanceId: live[0],
		})
		if err != nil {
			log.Fatal(err)
		}
		updateJob := realis.JobUpdateFromConfig(taskConfig).InstanceCount(5).RAM(128)

		result, err := r.StartJobUpdate(updateJob, "")
		if err != nil {
			log.Fatal(err)
		}

		jobUpdateKey := result.GetKey()
		_, err = r.JobUpdateMonitor(*jobUpdateKey, 5*time.Second, 6*time.Minute)
		if err != nil {
			log.Fatal(err)
		}

	case "pauseJobUpdate":
		key := job.JobKey()
		err := r.PauseJobUpdate(&aurora.JobUpdateKey{
			Job: &key,
			ID:  updateId,
		}, "")

		if err != nil {
			log.Fatal(err)
		}

	case "resumeJobUpdate":
		key := job.JobKey()
		err := r.ResumeJobUpdate(&aurora.JobUpdateKey{
			Job: &key,
			ID:  updateId,
		}, "")

		if err != nil {
			log.Fatal(err)
		}

	case "pulseJobUpdate":
		key := job.JobKey()
		resp, err := r.PulseJobUpdate(&aurora.JobUpdateKey{
			Job: &key,
			ID:  updateId,
		})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("PulseJobUpdate response: ", resp.String())

	case "updateDetails":
		key := job.JobKey()
		result, err := r.JobUpdateDetails(aurora.JobUpdateQuery{
			Key: &aurora.JobUpdateKey{
				Job: &key,
				ID:  updateId,
			},
			Limit: 1,
		})

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(result)

	case "abortUpdate":
		fmt.Println("Abort update")
		key := job.JobKey()
		err := r.AbortJobUpdate(aurora.JobUpdateKey{
			Job: &key,
			ID:  updateId,
		},
			"")

		if err != nil {
			log.Fatal(err)
		}

	case "rollbackUpdate":
		fmt.Println("Abort update")
		key := job.JobKey()
		err := r.RollbackJobUpdate(aurora.JobUpdateKey{
			Job: &key,
			ID:  updateId,
		},
			"")

		if err != nil {
			log.Fatal(err)
		}

	case "taskConfig":
		fmt.Println("Getting job info")
		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			log.Fatal(err)

		}
		key := job.JobKey()
		config, err := r.FetchTaskConfig(aurora.InstanceKey{
			JobKey:     &key,
			InstanceId: live[0],
		})

		if err != nil {
			log.Fatal(err)
		}

		log.Println(config.String())

	case "updatesummary":
		fmt.Println("Getting job update summary")
		key := job.JobKey()
		jobquery := &aurora.JobUpdateQuery{
			Role:   &key.Role,
			JobKey: &key,
		}
		updatesummary, err := r.GetJobUpdateSummaries(jobquery)
		if err != nil {
			log.Fatalf("error while getting update summary: %v", err)
		}

		fmt.Println(updatesummary)

	case "taskStatus":
		fmt.Println("Getting task status")
		key := job.JobKey()
		taskQ := &aurora.TaskQuery{
			Role:        &key.Role,
			Environment: &key.Environment,
			JobName:     &key.Name,
		}
		tasks, err := r.GetTaskStatus(taskQ)
		if err != nil {
			log.Fatalf("error: %+v\n ", err)
		}

		fmt.Printf("length: %d\n ", len(tasks))
		fmt.Printf("tasks: %+v\n", tasks)

	case "tasksWithoutConfig":
		fmt.Println("Getting task status")
		key := job.JobKey()
		taskQ := &aurora.TaskQuery{
			Role:        &key.Role,
			Environment: &key.Environment,
			JobName:     &key.Name,
		}
		tasks, err := r.GetTasksWithoutConfigs(taskQ)
		if err != nil {
			log.Fatalf("error: %+v\n ", err)
		}

		fmt.Printf("length: %d\n ", len(tasks))
		fmt.Printf("tasks: %+v\n", tasks)

	case "drainHosts":
		fmt.Println("Setting hosts to DRAINING")
		if hostList == "" {
			log.Fatal("No hosts specified to drain")
		}
		hosts := strings.Split(hostList, ",")
		_, err := r.DrainHosts(hosts...)
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

		// Monitor change to DRAINING and DRAINED mode
		hostResult, err := r.HostMaintenanceMonitor(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			5*time.Second,
			10*time.Second)
		if err != nil {
			for host, ok := range hostResult {
				if !ok {
					fmt.Printf("Host %s did not transtion into desired mode(s)\n", host)
				}
			}
			log.Fatalf("error: %+v\n", err.Error())
		}

	case "SLADrainHosts":
		fmt.Println("Setting hosts to DRAINING using SLA aware draining")
		if hostList == "" {
			log.Fatal("No hosts specified to drain")
		}
		hosts := strings.Split(hostList, ",")

		policy := aurora.SlaPolicy{PercentageSlaPolicy: &aurora.PercentageSlaPolicy{Percentage: 50.0}}

		_, err := r.SLADrainHosts(&policy, 30, hosts...)
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

		// Monitor change to DRAINING and DRAINED mode
		hostResult, err := r.HostMaintenanceMonitor(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			5*time.Second,
			10*time.Second)
		if err != nil {
			for host, ok := range hostResult {
				if !ok {
					fmt.Printf("Host %s did not transtion into desired mode(s)\n", host)
				}
			}
			log.Fatalf("error: %+v\n", err.Error())
		}

	case "endMaintenance":
		fmt.Println("Setting hosts to ACTIVE")
		if hostList == "" {
			log.Fatal("No hosts specified to drain")
		}
		hosts := strings.Split(hostList, ",")
		_, err := r.EndMaintenance(hosts...)
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

		// Monitor change to DRAINING and DRAINED mode
		hostResult, err := r.HostMaintenanceMonitor(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
			5*time.Second,
			10*time.Second)
		if err != nil {
			for host, ok := range hostResult {
				if !ok {
					fmt.Printf("Host %s did not transtion into desired mode(s)\n", host)
				}
			}
			log.Fatalf("error: %+v\n", err.Error())
		}

	case "getPendingReasons":
		fmt.Println("Getting pending reasons")
		key := job.JobKey()
		taskQ := &aurora.TaskQuery{
			Role:        &key.Role,
			Environment: &key.Environment,
			JobName:     &key.Name,
		}
		reasons, err := r.GetPendingReason(taskQ)
		if err != nil {
			log.Fatalf("error: %+v\n ", err)
		}

		fmt.Printf("length: %d\n ", len(reasons))
		fmt.Printf("tasks: %+v\n", reasons)

	case "getJobs":
		fmt.Println("GetJobs...role: ", role)
		result, err := r.GetJobs(role)
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}
		fmt.Println("map size: ", len(result.Configs))
		fmt.Println(result.String())

	case "snapshot":
		fmt.Println("Forcing scheduler to write snapshot to mesos replicated log")
		err := r.Snapshot()
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

	case "performBackup":
		fmt.Println("Writing Backup of Snapshot to file system")
		err := r.PerformBackup()
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

	case "forceExplicitRecon":
		fmt.Println("Force an explicit recon")
		err := r.ForceExplicitTaskReconciliation(nil)
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

	case "forceImplicitRecon":
		fmt.Println("Force an implicit recon")
		err := r.ForceImplicitTaskReconciliation()
		if err != nil {
			log.Fatalf("error: %+v\n", err.Error())
		}

	default:
		log.Fatal("Command not supported")
	}
}
