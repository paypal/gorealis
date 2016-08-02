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

	cmd := flag.String("cmd", "", "Action to execute on Apache Aurora Sched")
	executor := flag.String("executor", "thermos", "Executor to use, thermos by default")
	url := flag.String("url", "", "URL at which the Apache Aurora Scheduler exists [url]:[port]")
	updateId := flag.String("updateId", "", "Update ID to operate on")
	flag.Parse()

	//Create new configuration with default transport layer
	config, err := realis.NewDefaultConfig(*url)
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	// Needed for authorization for Vagrant
	realis.AddBasicAuth(&config, "aurora", "secret")
	r := realis.NewClient(config)
	defer r.Close()

	var job *realis.Job

	switch *executor {
	case "thermos":
		payload, err := ioutil.ReadFile("thermos_payload.json")

		if err != nil {
			fmt.Print("Error reading json config file: ", err)
			os.Exit(1)
		}
		job = realis.NewJob().Environment("prod").
			Role("vagrant").
			Name("hello_world_from_gorealis").
			ExecutorName(aurora.AURORA_EXECUTOR_NAME).
			ExecutorData(string(payload)).
			NumCpus(1).
			Ram(64).
			Disk(100).
			IsService(true).
			InstanceCount(1).
			AddPorts(1)
		break
	case "compose":
		job = realis.NewJob().Environment("prod").
			Role("vagrant").
			Name("docker-compose").
			ExecutorName("docker-compose-executor").
			ExecutorData("{}").
			NumCpus(1).
			Ram(64).
			Disk(100).
			IsService(false).
			InstanceCount(1).
			AddPorts(1).
			AddLabel("fileName", "sample-app/sample-app.yml").
			AddURI("https://dl.bintray.com/rdelvalle/mesos-compose-executor/sample-app.tar.gz", true, true)
		break
	default:
		fmt.Println("Only thermos and compose are supported for now")
		os.Exit(1)
	}

	switch *cmd {
	case "create":
		fmt.Println("Creating job")
		msg, err := r.CreateJob(job)
		if err != nil {
			fmt.Print(err)
		}

		fmt.Print(msg)
		break
	case "kill":
		fmt.Println("Killing job")

		msg, err := r.KillJob(job.JobKey())
		if err != nil {
			fmt.Print(err)
		}

		fmt.Print(msg)
		break
	case "restart":
		fmt.Println("Restarting job")
		msg, err := r.RestartJob(job.JobKey())
		if err != nil {
			fmt.Print(err)
		}

		fmt.Print(msg)
		break
	case "flexUp":
		fmt.Println("Flexing up job")
		msg, err := r.AddInstances(job.JobKey(), 5)
		if err != nil {
			fmt.Print(err)
		}
		fmt.Print(msg)
		break
	case "update":
		fmt.Println("Updating a job with a new name")
		updateJob := realis.NewUpdateJob(job)

		updateJob.InstanceCount(3).Ram(128)

		msg, err := r.StartJobUpdate(updateJob, "")
		if err != nil {
			fmt.Print(err)
		}
		fmt.Print(msg)
		break
	case "abortUpdate":
		fmt.Println("Abort update")
		msg, err := r.AbortJobUpdate(job.JobKey(), *updateId, "")
		if err != nil {
			fmt.Print(err)
		}
		fmt.Print(msg)
		break
	default:
		fmt.Println("Only Create, Kill, and Restart are supported now")
		os.Exit(1)
	}
}
