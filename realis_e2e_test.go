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

package realis_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"sync"

	"github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/stretchr/testify/assert"
)

var r realis.Realis
var monitor *realis.Monitor
var thermosPayload []byte

func TestMain(m *testing.M) {
	var err error

	// New configuration to connect to Vagrant image
	r, err = realis.NewRealisClient(realis.SchedulerUrl("http://192.168.33.7:8081"),
		realis.BasicAuth("aurora", "secret"),
		realis.TimeoutMS(20000))
	if err != nil {
		fmt.Println("Please run vagrant box before running test suite")
		os.Exit(1)
	}

	defer r.Close()

	// Create monitor
	monitor = &realis.Monitor{Client: r}

	thermosPayload, err = ioutil.ReadFile("examples/thermos_payload.json")
	if err != nil {
		fmt.Println("Error reading thermos payload file: ", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestBadEndpoint(t *testing.T) {

	// Attempt to connect to a bad endpoint
	r, err := realis.NewRealisClient(realis.SchedulerUrl("http://192.168.33.7:8081/scheduler/"),
		realis.TimeoutMS(200),
		realis.BackOff(&realis.Backoff{ // Reduce penalties for this test to make it quick
			Steps:    5,
			Duration: 1 * time.Second,
			Factor:   1.0,
			Jitter:   0.1}),
	)
	defer r.Close()

	taskQ := &aurora.TaskQuery{
		Role:        "no",
		Environment: "task",
		JobName:     "here",
	}

	_, err = r.GetTasksWithoutConfigs(taskQ)

	// Check that we do error out of retrying
	assert.Error(t, err)

}

func TestLeaderFromZK(t *testing.T) {
	cluster := realis.GetDefaultClusterFromZKUrl("192.168.33.7:2181")
	url, err := realis.LeaderFromZK(*cluster)

	assert.NoError(t, err)
	assert.Equal(t, "http://aurora.local:8081", url)
}

func TestRealisClient_ReestablishConn(t *testing.T) {

	// Test that we're able to tear down the old connection and create a new one.
	err := r.ReestablishConn()

	assert.NoError(t, err)
}

func TestGetCACerts(t *testing.T) {
	certs, err := realis.GetCerts("./examples/certs")
	assert.NoError(t, err)
	assert.Equal(t, len(certs.Subjects()), 2)

}

func TestRealisClient_CreateJob_Thermos(t *testing.T) {

	role := "vagrant"
	job := realis.NewJob().
		Environment("prod").
		Role(role).
		Name("create_thermos_job_test").
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
	assert.NoError(t, err)

	assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
	fmt.Printf("Create call took %d ns\n", (end.UnixNano() - start.UnixNano()))

	// Test Instances Monitor
	success, err := monitor.Instances(job.JobKey(), job.GetInstanceCount(), 1, 50)
	assert.True(t, success)
	assert.NoError(t, err)

	//Fetch all obs
	_, result, err := r.GetJobs(role)
	fmt.Printf("GetJobs length: %+v \n", len(result.Configs))
	assert.Equal(t, len(result.Configs), 1)
	assert.NoError(t, err)

	// Tasks must exist for it to, be killed
	t.Run("TestRealisClient_KillJob_Thermos", func(t *testing.T) {
		start := time.Now()
		resp, err := r.KillJob(job.JobKey())
		end := time.Now()
		assert.NoError(t, err)

		assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
		fmt.Printf("Kill call took %d ns\n", (end.UnixNano() - start.UnixNano()))
	})
}

func TestRealisClient_ScheduleCronJob_Thermos(t *testing.T) {

	thermosCronPayload, err := ioutil.ReadFile("examples/thermos_cron_payload.json")
	assert.NoError(t, err)

	job := realis.NewJob().
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

		assert.NoError(t, err)
		assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
		fmt.Printf("Schedule cron call took %d ns\n", (end.UnixNano() - start.UnixNano()))
	})

	t.Run("TestRealisClient_DeschedulerCronJob_Thermos", func(t *testing.T) {
		start := time.Now()
		resp, err := r.DescheduleCronJob(job.JobKey())
		end := time.Now()

		assert.NoError(t, err)
		assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)
		fmt.Printf("Deschedule cron call took %d ns\n", (end.UnixNano() - start.UnixNano()))
	})
}

func TestRealisClient_DrainHosts(t *testing.T) {
	hosts := []string{"192.168.33.7"}
	_, _, err := r.DrainHosts(hosts...)
	if err != nil {
		fmt.Printf("error: %+v\n", err.Error())
		os.Exit(1)
	}

	// Monitor change to DRAINING and DRAINED mode
	hostResults, err := monitor.HostMaintenance(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
		1,
		50)
	assert.Equal(t, map[string]bool{"192.168.33.7": true}, hostResults)
	assert.NoError(t, err)

	t.Run("TestRealisClient_MonitorNontransitioned", func(t *testing.T) {
		// Monitor change to DRAINING and DRAINED mode
		hostResults, err := monitor.HostMaintenance(
			append(hosts, "IMAGINARY_HOST"),
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			1,
			1)

		// Assert monitor returned an error that was not nil, and also a list of the non-transitioned hosts
		assert.Error(t, err)
		assert.Equal(t, map[string]bool{"192.168.33.7": true, "IMAGINARY_HOST": false}, hostResults)
	})

	t.Run("TestRealisClient_EndMaintenance", func(t *testing.T) {
		_, _, err := r.EndMaintenance(hosts...)
		if err != nil {
			fmt.Printf("error: %+v\n", err.Error())
			os.Exit(1)
		}

		// Monitor change to DRAINING and DRAINED mode
		_, err = monitor.HostMaintenance(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
			5,
			10)
		assert.NoError(t, err)
	})

}

// Test multiple go routines using a single connection
func TestRealisClient_SessionThreadSafety(t *testing.T) {

	// Create a single job
	job := realis.NewJob().
		Environment("prod").
		Role("vagrant").
		Name("create_thermos_job_test_multi").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(.25).
		RAM(4).
		Disk(10).
		InstanceCount(100) // Impossible amount to go live in the current vagrant default settings

	resp, err := r.CreateJob(job)
	assert.NoError(t, err)

	assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {

		wg.Add(1)

		// Launch multiple monitors that will poll every second
		go func() {
			defer wg.Done()

			// Test Schedule status monitor for terminal state and timing out after 30 seconds
			success, err := monitor.ScheduleStatus(job.JobKey(), job.GetInstanceCount(), aurora.LIVE_STATES, 1, 30)
			assert.False(t, success)
			assert.Error(t, err)

			resp, err := r.KillJob(job.JobKey())
			assert.NoError(t, err)

			assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)

		}()
	}

	wg.Wait()
}
