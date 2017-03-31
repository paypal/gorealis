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

package realis

import (
	"fmt"
	"github.com/rdelval/gorealis/gen-go/apache/aurora"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var r Realis
var thermosPayload []byte

func TestMain(m *testing.M) {
	var err error

	// New configuration to connect to Vagrant image
	r, err = NewDefaultClientUsingUrl("http://192.168.33.7:8081","aurora", "secret")
	if err != nil {
		fmt.Println("Please run vagrant box before running test suite")
		os.Exit(1)
	}

	thermosPayload, err = ioutil.ReadFile("examples/thermos_payload.json")
	if err != nil {
		fmt.Println("Error reading thermos payload file: ", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestRealisClient_CreateJob_Thermos(t *testing.T) {

	job := NewJob().
		Environment("prod").
		Role("vagrant").
		Name("create_job_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(1).
		RAM(64).
		Disk(100).
		IsService(true).
		InstanceCount(1).
		AddPorts(1)

	start := time.Now()
	resp, err := r.CreateJob(job)
	end := time.Now()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
	fmt.Printf("Create call took %d ns\n", (end.UnixNano()- start.UnixNano()))

	// Tasks must exist for it to be killed
	t.Run("TestRealisClient_KillJob_Thermos", func(t *testing.T) {
		start := time.Now()
		resp, err := r.KillJob(job.JobKey())
		end := time.Now()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
		fmt.Printf("Kill call took %d ns\n", (end.UnixNano()- start.UnixNano()))
	})
}

func TestRealisClient_ScheduleCronJob_Thermos(t *testing.T) {

	thermosCronPayload, err := ioutil.ReadFile("examples/thermos_cron_payload.json")
	if err != nil {
		fmt.Println("Error reading thermos payload file: ", err)
		os.Exit(1)
	}

	job := NewJob().
		Environment("prod").
		Role("vagrant").
		Name("cronsched_job_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosCronPayload)).
		CPU(1).
		RAM(64).
		Disk(100).
		IsService(true).
		InstanceCount(1).
		AddPorts(1).
		CronSchedule("* * * * *").
		IsService(false)

	resp, err := r.ScheduleCronJob(job)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)

	t.Run("TestRealisClient_StartCronJob_Thermos", func(t *testing.T) {
		start := time.Now()
		resp, err := r.StartCronJob(job.JobKey())
		end := time.Now()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
		fmt.Printf("Schedule cron call took %d ns\n", (end.UnixNano()- start.UnixNano()))
	})

	t.Run("TestRealisClient_DeschedulerCronJob_Thermos", func(t *testing.T) {
		start := time.Now()
		resp, err := r.DescheduleCronJob(job.JobKey())
		end := time.Now()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
		fmt.Printf("Deschedule cron call took %d ns\n", (end.UnixNano()- start.UnixNano()))
	})
}
