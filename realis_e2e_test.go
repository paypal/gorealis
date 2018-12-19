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
	"sync"
	"testing"
	"time"

	realis "github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var r *realis.Client
var thermosPayload []byte

func TestMain(m *testing.M) {
	var err error

	// New configuration to connect to docker container
	r, err = realis.NewClient(realis.SchedulerUrl("http://192.168.33.7:8081"),
		realis.BasicAuth("aurora", "secret"),
		realis.Timeout(20*time.Second))

	if err != nil {
		fmt.Println("Please run docker-compose up -d before running test suite")
		os.Exit(1)
	}

	defer r.Close()

	thermosPayload, err = ioutil.ReadFile("examples/thermos_payload.json")
	if err != nil {
		fmt.Println("Error reading thermos payload file: ", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestNonExistentEndpoint(t *testing.T) {
	backoff := realis.Backoff{ // Reduce penalties for this test to make it quick
		Steps:    5,
		Duration: 1 * time.Second,
		Factor:   1.0,
		Jitter:   0.1}

	// Attempt to connect to a bad endpoint
	r, err := realis.NewClient(realis.SchedulerUrl("http://doesntexist.com:8081/api"),
		realis.Timeout(200*time.Millisecond),
		realis.BackOff(backoff),
	)
	defer r.Close()

	taskQ := &aurora.TaskQuery{}

	_, err = r.GetTasksWithoutConfigs(taskQ)

	// Check that we do error out of retrying
	assert.Error(t, err)

	// Check that the error before this one was a a retry error
	// TODO: Consider bubbling up timeout behaving error all the way up to the user.
	retryErr := realis.ToRetryCount(errors.Cause(err))
	assert.NotNil(t, retryErr, "error passed in is not a retry error")

	assert.Equal(t, backoff.Steps, retryErr.RetryCount(), "retry count is incorrect")

}

func TestBadCredentials(t *testing.T) {
	r, err := realis.NewClient(realis.SchedulerUrl("http://192.168.33.7:8081"),
		realis.BasicAuth("incorrect", "password"),
		realis.Debug())
	defer r.Close()

	assert.NoError(t, err)

	job := realis.NewJob().
		Environment("prod").
		Role("vagrant").
		Name("create_thermos_job_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(.5).
		RAM(64).
		Disk(100).
		IsService(true).
		InstanceCount(2).
		AddPorts(1)

	assert.Error(t, r.CreateJob(job))
}

func TestThriftBinary(t *testing.T) {
	r, err := realis.NewClient(realis.SchedulerUrl("http://192.168.33.7:8081"),
		realis.BasicAuth("aurora", "secret"),
		realis.Timeout(20*time.Second),
		realis.ThriftBinary())

	assert.NoError(t, err)
	defer r.Close()

	role := "all"
	taskQ := &aurora.TaskQuery{
		Role: &role,
	}

	// Perform a simple API call to test Thrift Binary
	_, err = r.GetTasksWithoutConfigs(taskQ)

	assert.NoError(t, err)

}

func TestThriftJSON(t *testing.T) {
	r, err := realis.NewClient(realis.SchedulerUrl("http://192.168.33.7:8081"),
		realis.BasicAuth("aurora", "secret"),
		realis.Timeout(20*time.Second),
		realis.ThriftJSON())

	assert.NoError(t, err)

	role := "all"
	taskQ := &aurora.TaskQuery{
		Role: &role,
	}

	// Perform a simple API call to test Thrift Binary
	_, err = r.GetTasksWithoutConfigs(taskQ)

	assert.NoError(t, err)

	r.Close()

}

func TestNoopLogger(t *testing.T) {
	r, err := realis.NewClient(realis.SchedulerUrl("http://192.168.33.7:8081"),
		realis.BasicAuth("aurora", "secret"),
		realis.SetLogger(realis.NoopLogger{}))

	assert.NoError(t, err)

	role := "all"
	taskQ := &aurora.TaskQuery{
		Role: &role,
	}

	// Perform a simple API call to test Thrift Binary
	_, err = r.GetTasksWithoutConfigs(taskQ)

	assert.NoError(t, err)

	r.Close()
}

func TestLeaderFromZK(t *testing.T) {
	cluster := realis.GetDefaultClusterFromZKUrl("192.168.33.2:2181")
	url, err := realis.LeaderFromZK(*cluster)

	assert.NoError(t, err)
	assert.Equal(t, "http://192.168.33.7:8081", url)

}
func TestInvalidAuroraURL(t *testing.T) {
	for _, url := range []string{
		"http://doesntexist.com:8081/apitest",
		"test://doesntexist.com:8081",
		"https://doesntexist.com:8081/testing/api",
	} {
		r, err := realis.NewClient(realis.SchedulerUrl(url))
		assert.Error(t, err)
		assert.Nil(t, r)
	}
}

func TestValidAuroraURL(t *testing.T) {
	for _, url := range []string{
		"http://domain.com:8081/api",
		"https://domain.com:8081/api",
		"domain.com:8081",
		"domain.com",
		"192.168.33.7",
		"192.168.33.7:8081",
		"192.168.33.7:8081/api",
	} {
		r, err := realis.NewClient(realis.SchedulerUrl(url))
		assert.NoError(t, err)
		assert.NotNil(t, r)
	}
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
		CPU(.5).
		RAM(64).
		Disk(100).
		IsService(true).
		InstanceCount(2).
		AddPorts(1)

	err := r.CreateJob(job)
	assert.NoError(t, err)

	// Test Instances Monitor
	success, err := r.InstancesMonitor(job.JobKey(), job.GetInstanceCount(), 1*time.Second, 50*time.Second)
	assert.True(t, success)
	assert.NoError(t, err)

	// Fetch all Jobs
	result, err := r.GetJobs(role)
	fmt.Printf("GetJobs length: %+v \n", len(result.Configs))
	assert.Len(t, result.Configs, 1)
	assert.NoError(t, err)

	// Test asking the scheduler to perform a Snapshot
	t.Run("TestRealisClient_Snapshot", func(t *testing.T) {
		err := r.Snapshot()
		assert.NoError(t, err)
	})

	// Test asking the scheduler to backup a Snapshot
	t.Run("TestRealisClient_PerformBackup", func(t *testing.T) {
		err := r.PerformBackup()
		assert.NoError(t, err)
	})

	// Tasks must exist for it to, be killed
	t.Run("TestRealisClient_KillJob_Thermos", func(t *testing.T) {
		err := r.KillJob(job.JobKey())
		assert.NoError(t, err)

		success, err := r.InstancesMonitor(job.JobKey(), 0, 1*time.Second, 50*time.Second)
		assert.True(t, success)
		assert.NoError(t, err)
	})
}

// Test configuring an executor that doesn't exist for CreateJob API
func TestRealisClient_CreateJob_ExecutorDoesNotExist(t *testing.T) {

	// Create a single job
	job := realis.NewJob().
		Environment("prod").
		Role("vagrant").
		Name("executordoesntexist").
		ExecutorName("idontexist").
		ExecutorData("").
		CPU(.25).
		RAM(4).
		Disk(10).
		InstanceCount(1)

	err := r.CreateJob(job)
	assert.Error(t, err)
}

// Test configuring an executor that doesn't exist for CreateJob API
func TestRealisClient_GetPendingReason(t *testing.T) {

	env := "prod"
	role := "vagrant"
	name := "pending_reason_test"

	// Create a single job
	job := realis.NewJob().
		Environment(env).
		Role(role).
		Name(name).
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(1000).
		RAM(64).
		Disk(100).
		InstanceCount(1)

	err := r.CreateJob(job)
	assert.NoError(t, err)

	taskQ := &aurora.TaskQuery{
		Role:        &role,
		Environment: &env,
		JobName:     &name,
	}

	reasons, err := r.GetPendingReason(taskQ)
	assert.NoError(t, err)
	assert.Len(t, reasons, 1)

	err = r.KillJob(job.JobKey())
	assert.NoError(t, err)
}

func TestRealisClient_CreateService_WithPulse_Thermos(t *testing.T) {

	fmt.Println("Creating service")
	role := "vagrant"
	job := realis.NewJobUpdate().
		Environment("prod").
		Role(role).
		Name("create_thermos_job_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(.5).
		RAM(64).
		Disk(100).
		IsService(true).
		InstanceCount(2).
		AddPorts(1).
		AddLabel("currentTime", time.Now().String()).
		PulseIntervalTimeout(30 * time.Millisecond).
		BatchSize(1).WaitForBatchCompletion(true)

	pulse := int32(30)
	timeout := 300
	result, err := r.CreateService(job)
	fmt.Println(result.String())

	assert.NoError(t, err)

	updateQ := aurora.JobUpdateQuery{
		Key:   result.GetKey(),
		Limit: 1,
	}

	var updateDetails []*aurora.JobUpdateDetails

	for i := 0; i*int(pulse) <= timeout; i++ {

		pulseStatus, err := r.PulseJobUpdate(result.GetKey())

		assert.Nil(t, err)
		if pulseStatus != aurora.JobUpdatePulseStatus_OK && pulseStatus != aurora.JobUpdatePulseStatus_FINISHED {
			assert.Fail(t, "Pulse update status received doesn't exist")
		}

		updateDetails, err = r.JobUpdateDetails(updateQ)
		assert.Nil(t, err)

		assert.Equal(t, len(updateDetails), 1, "No update matching query found")
		status := updateDetails[0].Update.Summary.State.Status

		if _, ok := realis.ActiveJobUpdateStates[status]; !ok {

			// Rolled forward is the only state in which an update has been successfully updated
			// if we encounter an inactive state and it is not at rolled forward, update failed
			if status == aurora.JobUpdateStatus_ROLLED_FORWARD {
				fmt.Println("Update succeeded")
				break
			} else {
				fmt.Println("Update failed")
				break
			}
		}
		fmt.Println("Polling, update still active...")
	}

	t.Run("TestRealisClient_KillJob_Thermos", func(t *testing.T) {
		err := r.AbortJobUpdate(*updateDetails[0].GetUpdate().GetSummary().GetKey(), "")
		assert.NoError(t, err)
		err = r.KillJob(job.JobKey())
		assert.NoError(t, err)
	})

}

// Test configuring an executor that doesn't exist for CreateJob API
func TestRealisClient_CreateService(t *testing.T) {

	// Create a single job
	job := realis.NewJobUpdate().
		Environment("prod").
		Role("vagrant").
		Name("create_service_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(.25).
		RAM(4).
		Disk(10).
		InstanceCount(3).
		WatchTime(20 * time.Second).
		IsService(true).
		BatchSize(2)

	result, err := r.CreateService(job)

	assert.NoError(t, err)
	assert.NotNil(t, result)

	var ok bool
	var mErr error

	if ok, mErr = r.JobUpdateMonitor(*result.GetKey(), 5*time.Second, 4*time.Minute); !ok || mErr != nil {
		// Update may already be in a terminal state so don't check for error
		err := r.AbortJobUpdate(*result.GetKey(), "Monitor timed out.")

		err = r.KillJob(job.JobKey())

		assert.NoError(t, err)
	}

	assert.True(t, ok)
	assert.NoError(t, mErr)

	// Kill task test task after confirming it came up fine
	err = r.KillJob(job.JobKey())

	assert.NoError(t, err)
}

// Test configuring an executor that doesn't exist for CreateJob API
func TestRealisClient_CreateService_ExecutorDoesNotExist(t *testing.T) {

	// Create a single job
	jobUpdate := realis.NewJobUpdate().
		Environment("prod").
		Role("vagrant").
		Name("executordoesntexist").
		ExecutorName("idontexist").
		ExecutorData("").
		CPU(.25).
		RAM(4).
		Disk(10).
		InstanceCount(3)

	result, err := r.CreateService(jobUpdate)

	assert.Error(t, err)
	assert.Nil(t, result)
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

	err = r.ScheduleCronJob(job)
	assert.NoError(t, err)

	t.Run("TestRealisClient_StartCronJob_Thermos", func(t *testing.T) {
		err := r.StartCronJob(job.JobKey())
		assert.NoError(t, err)
	})

	t.Run("TestRealisClient_DeschedulerCronJob_Thermos", func(t *testing.T) {
		err := r.DescheduleCronJob(job.JobKey())
		assert.NoError(t, err)
	})
}
func TestRealisClient_StartMaintenance(t *testing.T) {
	hosts := []string{"localhost"}
	_, err := r.StartMaintenance(hosts...)
	assert.NoError(t, err)

	// Monitor change to DRAINING and DRAINED mode
	hostResults, err := r.HostMaintenanceMonitor(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_SCHEDULED},
		1*time.Second,
		50*time.Second)
	assert.Equal(t, map[string]bool{"localhost": true}, hostResults)
	assert.NoError(t, err)

	_, err = r.EndMaintenance(hosts...)
	assert.NoError(t, err)

	// Monitor change to DRAINING and DRAINED mode
	_, err = r.HostMaintenanceMonitor(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
		5*time.Second,
		10*time.Second)
	assert.NoError(t, err)
}

func TestRealisClient_DrainHosts(t *testing.T) {
	hosts := []string{"localhost"}
	_, err := r.DrainHosts(hosts...)
	assert.NoError(t, err)

	// Monitor change to DRAINING and DRAINED mode
	hostResults, err := r.HostMaintenanceMonitor(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
		1*time.Second,
		50*time.Second)
	assert.Equal(t, map[string]bool{"localhost": true}, hostResults)
	assert.NoError(t, err)

	t.Run("TestRealisClient_MonitorNontransitioned", func(t *testing.T) {
		// Monitor change to DRAINING and DRAINED mode
		hostResults, err := r.HostMaintenanceMonitor(
			append(hosts, "IMAGINARY_HOST"),
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			1*time.Second,
			1*time.Second)

		// Assert monitor returned an error that was not nil, and also a list of the non-transitioned hosts
		assert.Error(t, err)
		assert.Equal(t, map[string]bool{"localhost": true, "IMAGINARY_HOST": false}, hostResults)
	})

	t.Run("TestRealisClient_EndMaintenance", func(t *testing.T) {
		_, err := r.EndMaintenance(hosts...)
		assert.NoError(t, err)

		// Monitor change to DRAINING and DRAINED mode
		_, err = r.HostMaintenanceMonitor(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
			5*time.Second,
			10*time.Second)
		assert.NoError(t, err)
	})

}

func TestRealisClient_SLADrainHosts(t *testing.T) {
	hosts := []string{"localhost"}
	policy := aurora.SlaPolicy{PercentageSlaPolicy: &aurora.PercentageSlaPolicy{Percentage: 50.0}}

	_, err := r.SLADrainHosts(&policy, 30, hosts...)
	if err != nil {
		fmt.Printf("error: %+v\n", err.Error())
		os.Exit(1)
	}

	// Monitor change to DRAINING and DRAINED mode
	hostResults, err := r.HostMaintenanceMonitor(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
		1*time.Second,
		50*time.Second)
	assert.Equal(t, map[string]bool{"localhost": true}, hostResults)
	assert.NoError(t, err)

	_, err = r.EndMaintenance(hosts...)
	assert.NoError(t, err)

	// Monitor change to DRAINING and DRAINED mode
	_, err = r.HostMaintenanceMonitor(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
		5*time.Second,
		10*time.Second)
	assert.NoError(t, err)
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
		InstanceCount(1000) // Impossible amount to go live in any sane machine

	err := r.CreateJob(job)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {

		wg.Add(1)

		// Launch multiple monitors that will poll every second
		go func() {
			defer wg.Done()

			// Test Schedule status monitor for terminal state and timing out after 30 seconds
			success, err := r.ScheduleStatusMonitor(job.JobKey(), job.GetInstanceCount(), aurora.LIVE_STATES, 1, 30)
			assert.False(t, success)
			assert.Error(t, err)

			err = r.KillJob(job.JobKey())
			assert.NoError(t, err)

		}()
	}

	wg.Wait()
}

// Test setting and getting the quota
func TestRealisClient_SetQuota(t *testing.T) {
	var cpu = 3.5
	var ram int64 = 20480
	var disk int64 = 10240
	err := r.SetQuota("vagrant", &cpu, &ram, &disk)
	assert.NoError(t, err)
	t.Run("TestRealisClient_GetQuota", func(t *testing.T) {
		// Test GetQuota based on previously set values
		var result *aurora.GetQuotaResult_
		quotaResult, err := r.GetQuota("vagrant")

		assert.NoError(t, err)
		for _, res := range quotaResult.GetQuota().GetResources() {
			switch true {
			case res.DiskMb != nil:
				assert.Equal(t, disk, *res.DiskMb)
				break
			case res.NumCpus != nil:
				assert.Equal(t, cpu, *res.NumCpus)
				break
			case res.RamMb != nil:
				assert.Equal(t, ram, *res.RamMb)
				break
			}
		}
		fmt.Print("GetQuota Result", result.String())
	})
}

func TestRealisClient_ForceImplicitTaskReconciliation(t *testing.T) {
	err := r.ForceImplicitTaskReconciliation()
	assert.NoError(t, err)
}

func TestRealisClient_ForceExplicitTaskReconciliation(t *testing.T) {
	// Default value
	err := r.ForceExplicitTaskReconciliation(nil)
	assert.NoError(t, err)

	// Custom batch value
	var batchSize int32 = 32
	err = r.ForceExplicitTaskReconciliation(&batchSize)
	assert.NoError(t, err)
}
