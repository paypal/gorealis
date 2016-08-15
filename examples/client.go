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
	"gen-go/apache/aurora"
	"github.com/rdelval/gorealis"
	"io/ioutil"
	"os"
)

func main() {

	cmd := flag.String("cmd", "", "Job request type to send to Aurora Scheduler")
	executor := flag.String("executor", "thermos", "Executor to use")
	url := flag.String("url", "", "URL at which the Aurora Scheduler exists as [url]:[port]")
	updateId := flag.String("updateId", "", "Update ID to operate on")
	username := flag.String("username", "aurora", "Username to use for authorization")
	password := flag.String("password", "secret", "Password to use for authorization")
	flag.Parse()

	//Create new configuration with default transport layer
	config, err := realis.NewDefaultConfig(*url)
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	// Configured for vagrant
	realis.AddBasicAuth(&config, *username, *password)
	r := realis.NewClient(config)
	defer r.Close()

	var job *realis.Job

	switch *executor {
	case "thermos":
		payload, err := ioutil.ReadFile("examples/thermos_payload.json")
		if err != nil {
			fmt.Print("Error reading json config file: ", err)
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
			CPU(1).
			RAM(64).
			Disk(100).
			IsService(false).
			InstanceCount(1).
			AddPorts(1).
			AddLabel("fileName", "sample-app/docker-compose.yml").
			AddURIs(true, true, "https://github.com/mesos/docker-compose-executor/releases/download/0.1.0/sample-app.tar.gz")
		break
	default:
		fmt.Println("Only thermos and compose are supported for now")
		os.Exit(1)
	}

	switch *cmd {
	case "create":
		fmt.Println("Creating job")
		response, err := r.CreateJob(job)
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		fmt.Print(response.String())
		break
	case "kill":
		fmt.Println("Killing job")

		response, err := r.KillJob(job.JobKey())
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		fmt.Print(response.String())
		break
	case "restart":
		fmt.Println("Restarting job")
		response, err := r.RestartJob(job.JobKey())
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		fmt.Print(response.String())
		break
	case "flexUp":
		fmt.Println("Flexing up job")
		response, err := r.AddInstances(&aurora.InstanceKey{job.JobKey(), 0}, 5)
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}
		fmt.Print(response.String())
		break
	case "update":
		fmt.Println("Updating a job with a new name")
		updateJob := realis.NewUpdateJob(job)

		updateJob.InstanceCount(3).RAM(128)

		resposne, err := r.StartJobUpdate(updateJob, "")
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}
		fmt.Print(resposne.String())
		break
	case "abortUpdate":
		fmt.Println("Abort update")
		response, err := r.AbortJobUpdate(job.JobKey(), *updateId, "")
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}
		fmt.Print(response.String())
		break
	default:
		fmt.Println("Only create, kill, restart, flexUp, update, and abortUpdate are supported now")
		os.Exit(1)
	}
}
