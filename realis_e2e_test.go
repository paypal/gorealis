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

	"github.com/apache/thrift/lib/go/thrift"
	realis "github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var r realis.Realis
var monitor *realis.Monitor
var thermosPayload []byte

const auroraURL = "http://192.168.33.7:8081"

func TestMain(m *testing.M) {
	var err error

	// New configuration to connect to docker container
	r, err = realis.NewRealisClient(realis.SchedulerUrl(auroraURL),
		realis.BasicAuth("aurora", "secret"),
		realis.TimeoutMS(20000))

	if err != nil {
		fmt.Println("Please run docker-compose up -d before running test suite")
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

func TestNonExistentEndpoint(t *testing.T) {
	backoff := realis.Backoff{ // Reduce penalties for this test to make it quick
		Steps:    5,
		Duration: 1 * time.Second,
		Factor:   1.0,
		Jitter:   0.1}

	// Attempt to connect to a bad endpoint
	r, err := realis.NewRealisClient(realis.SchedulerUrl("http://192.168.33.7:8081/doesntexist/"),
		realis.TimeoutMS(200),
		realis.BackOff(backoff),
	)

	assert.NoError(t, err)
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

func TestThriftBinary(t *testing.T) {
	r, err := realis.NewRealisClient(realis.SchedulerUrl(auroraURL),
		realis.BasicAuth("aurora", "secret"),
		realis.TimeoutMS(20000),
		realis.ThriftBinary())

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

func TestThriftJSON(t *testing.T) {
	r, err := realis.NewRealisClient(realis.SchedulerUrl(auroraURL),
		realis.BasicAuth("aurora", "secret"),
		realis.TimeoutMS(20000),
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
	r, err := realis.NewRealisClient(realis.SchedulerUrl(auroraURL),
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

	// Address stored inside of ZK might be different than the one we connect to in our tests.
	assert.Equal(t, "http://192.168.33.7:8081", url)
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
		CPU(.1).
		RAM(16).
		Disk(50).
		IsService(true).
		InstanceCount(2).
		AddPorts(1)

	_, err := r.CreateJob(job)
	assert.NoError(t, err)

	// Test Instances Monitor
	success, err := monitor.Instances(job.JobKey(), job.GetInstanceCount(), 1, 50)
	assert.True(t, success)
	assert.NoError(t, err)

	// Fetch all Jobs
	_, result, err := r.GetJobs(role)
	assert.Len(t, result.Configs, 1)
	assert.NoError(t, err)

	// Test asking the scheduler to perform a Snpshot
	t.Run("Snapshot", func(t *testing.T) {
		err := r.Snapshot()
		assert.NoError(t, err)
	})

	// Test asking the scheduler to backup a Snapshot
	t.Run("PerformBackup", func(t *testing.T) {
		err := r.PerformBackup()
		assert.NoError(t, err)
	})

	t.Run("GetTaskStatus", func(t *testing.T) {
		status, err := r.GetTaskStatus(&aurora.TaskQuery{
			JobKeys:  []*aurora.JobKey{job.JobKey()},
			Statuses: []aurora.ScheduleStatus{aurora.ScheduleStatus_RUNNING}})
		assert.NoError(t, err)
		assert.NotNil(t, status)
		assert.Len(t, status, 2)

		// TODO: Assert that assigned task matches the configuration of the task scheduled
	})

	t.Run("AddInstances", func(t *testing.T) {
		_, err := r.AddInstances(aurora.InstanceKey{JobKey: job.JobKey(), InstanceId: 0}, 2)
		assert.NoError(t, err)
		success, err := monitor.Instances(job.JobKey(), 4, 1, 50)
		assert.True(t, success)
		assert.NoError(t, err)
	})

	t.Run("KillInstances", func(t *testing.T) {
		_, err := r.KillInstances(job.JobKey(), 2, 3)
		assert.NoError(t, err)
		success, err := monitor.Instances(job.JobKey(), 2, 1, 50)
		assert.True(t, success)
		assert.NoError(t, err)
	})

	t.Run("RestartInstances", func(t *testing.T) {
		_, err := r.RestartInstances(job.JobKey(), 0)
		assert.NoError(t, err)
		success, err := monitor.Instances(job.JobKey(), 2, 1, 50)
		assert.True(t, success)
		assert.NoError(t, err)
	})

	t.Run("RestartJobs", func(t *testing.T) {
		_, err := r.RestartJob(job.JobKey())
		assert.NoError(t, err)
		success, err := monitor.Instances(job.JobKey(), 2, 1, 50)
		assert.True(t, success)
		assert.NoError(t, err)
	})

	// Tasks must exist for it to, be killed
	t.Run("KillJob", func(t *testing.T) {
		_, err := r.KillJob(job.JobKey())
		assert.NoError(t, err)
		success, err := monitor.Instances(job.JobKey(), 0, 1, 50)
		assert.True(t, success)
		assert.NoError(t, err)
	})

	t.Run("Duplicate_Metadata", func(t *testing.T) {
		job.Name("thermos_duplicate_metadata").
			AddLabel("hostname", "cookie").
			AddLabel("hostname", "candy").
			AddLabel("hostname", "popcorn").
			AddLabel("hostname", "chips").
			AddLabel("chips", "chips")

		_, err := r.CreateJob(job)
		assert.NoError(t, err)

		success, err := monitor.Instances(job.JobKey(), 2, 1, 50)
		assert.True(t, success)
		assert.NoError(t, err)

		_, err = r.KillJob(job.JobKey())
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

	resp, err := r.CreateJob(job)
	assert.Error(t, err)
	assert.Equal(t, aurora.ResponseCode_INVALID_REQUEST, resp.GetResponseCode())
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

	resp, err := r.CreateJob(job)
	assert.NoError(t, err)
	assert.Equal(t, aurora.ResponseCode_OK, resp.ResponseCode)

	taskQ := &aurora.TaskQuery{
		Role:        &role,
		Environment: &env,
		JobName:     &name,
	}

	reasons, err := r.GetPendingReason(taskQ)
	assert.NoError(t, err)
	assert.Len(t, reasons, 1)

	_, err = r.KillJob(job.JobKey())
	assert.NoError(t, err)
}

func TestRealisClient_CreateService_WithPulse_Thermos(t *testing.T) {

	fmt.Println("Creating service")
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
		InstanceCount(1).
		AddPorts(1).
		AddLabel("currentTime", time.Now().String())

	settings := realis.NewUpdateSettings()
	settings.BlockIfNoPulsesAfterMs = thrift.Int32Ptr(500)
	settings.UpdateGroupSize = 1
	settings.MinWaitInInstanceRunningMs = 5000
	settings.WaitForBatchCompletion = true
	job.InstanceCount(2)

	_, result, err := r.CreateService(job, settings)
	assert.NoError(t, err)

	updateQ := aurora.JobUpdateQuery{
		Key:   result.GetKey(),
		Limit: 1,
	}

	var updateDetails []*aurora.JobUpdateDetails

	ticker := time.NewTicker(time.Second * 1)
	timer := time.NewTimer(time.Minute * 6)
	defer ticker.Stop()
	defer timer.Stop()

pulseLoop:
	for {
		select {
		case <-ticker.C:

			_, err = r.PulseJobUpdate(result.GetKey())
			assert.Nil(t, err, "unable to pulse job update")

			respDetail, err := r.JobUpdateDetails(updateQ)
			assert.Nil(t, err)

			updateDetails = response.JobUpdateDetails(respDetail)
			assert.Len(t, updateDetails, 1, "No update found")

			status := updateDetails[0].Update.Summary.State.Status
			if _, ok := realis.ActiveJobUpdateStates[status]; !ok {

				// Rolled forward is the only state in which an update has been successfully updated
				// if we encounter an inactive state and it is not at rolled forward, update failed
				if status == aurora.JobUpdateStatus_ROLLED_FORWARD {
					fmt.Println("update succeed")
					break pulseLoop
				} else {
					fmt.Println("update failed")
					break pulseLoop
				}
			}
		case <-timer.C:
			_, err := r.AbortJobUpdate(*updateDetails[0].GetUpdate().GetSummary().GetKey(), "")
			assert.NoError(t, err)
			_, err = r.KillJob(job.JobKey())
			require.NoError(t, err, "timed out during pulse update test")
		}
	}

	_, err = r.KillJob(job.JobKey())
	assert.NoError(t, err)
}

// Test configuring an executor that doesn't exist for CreateJob API
func TestRealisClient_CreateService(t *testing.T) {

	// Create a single job
	job := realis.NewJob().
		Environment("prod").
		Role("vagrant").
		Name("create_service_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(.25).
		RAM(4).
		Disk(10).
		InstanceCount(3).
		IsService(true)

	settings := realis.NewUpdateSettings()
	settings.UpdateGroupSize = 2
	settings.MinWaitInInstanceRunningMs = 5000
	job.InstanceCount(3)

	_, result, err := r.CreateService(job, settings)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test asking the scheduler to backup a Snapshot
	t.Run("PauseJobUpdate", func(t *testing.T) {
		_, err = r.PauseJobUpdate(result.GetKey(), "")
		assert.NoError(t, err)
	})

	t.Run("ResumeJobUpdate", func(t *testing.T) {
		_, err = r.ResumeJobUpdate(result.GetKey(), "")
		assert.NoError(t, err)
	})

	var ok bool
	var mErr error

	if ok, mErr = monitor.JobUpdate(*result.GetKey(), 5, 240); !ok || mErr != nil {
		// Update may already be in a terminal state so don't check for error
		_, err := r.AbortJobUpdate(*result.GetKey(), "Monitor timed out.")
		_, err = r.KillJob(job.JobKey())
		assert.NoError(t, err)
	}

	assert.True(t, ok)
	assert.NoError(t, mErr)

	// Kill task test task after confirming it came up fine
	_, err = r.KillJob(job.JobKey())
	assert.NoError(t, err)

	success, err := monitor.Instances(job.JobKey(), 0, 1, 50)
	assert.True(t, success)

	// Create a client which will timeout and close the connection before receiving an answer
	timeoutClient, err := realis.NewRealisClient(realis.SchedulerUrl(auroraURL),
		realis.BasicAuth("aurora", "secret"),
		realis.TimeoutMS(10))
	assert.NoError(t, err)
	defer timeoutClient.Close()

	// Test case where http connection timeouts out.
	t.Run("TimeoutError", func(t *testing.T) {
		job.Name("createService_timeout")

		// Make sure a timedout error was returned
		_, _, err = timeoutClient.CreateService(job, settings)
		assert.Error(t, err)
		assert.True(t, realis.IsTimeout(err))

		updateReceivedQuery := aurora.JobUpdateQuery{
			Role:           &job.JobKey().Role,
			JobKey:         job.JobKey(),
			UpdateStatuses: aurora.ACTIVE_JOB_UPDATE_STATES,
			Limit:          1}

		updateSummaries, err := monitor.JobUpdateQuery(updateReceivedQuery, time.Second*1, time.Second*50)
		assert.NoError(t, err)

		assert.Len(t, updateSummaries, 1)

		r.AbortJobUpdate(*updateSummaries[0].Key, "Cleaning up")
		_, err = r.KillJob(job.JobKey())
		assert.NoError(t, err)

	})

	// Test case where http connection timeouts out.
	t.Run("TimeoutError_BadPayload", func(t *testing.T) {
		// Illegal payload
		job.InstanceCount(-1)
		job.Name("createService_timeout_bad_payload")

		// Make sure a timedout error was returned
		_, _, err = timeoutClient.CreateService(job, settings)
		assert.Error(t, err)
		assert.True(t, realis.IsTimeout(err))

		summary, err := r.GetJobUpdateSummaries(
			&aurora.JobUpdateQuery{
				Role:           &job.JobKey().Role,
				JobKey:         job.JobKey(),
				UpdateStatuses: aurora.ACTIVE_JOB_UPDATE_STATES})
		assert.NoError(t, err)

		// Payload should have been rejected, no update should exist
		assert.Len(t, summary.GetResult_().GetGetJobUpdateSummariesResult_().GetUpdateSummaries(), 0)
	})
}

// Test configuring an executor that doesn't exist for CreateJob API
func TestRealisClient_CreateService_ExecutorDoesNotExist(t *testing.T) {

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

	settings := realis.NewUpdateSettings()
	job.InstanceCount(3)

	resp, result, err := r.CreateService(job, settings)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, aurora.ResponseCode_INVALID_REQUEST, resp.GetResponseCode())
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

	_, err = r.ScheduleCronJob(job)
	assert.NoError(t, err)

	t.Run("Start", func(t *testing.T) {
		_, err := r.StartCronJob(job.JobKey())
		assert.NoError(t, err)
	})

	t.Run("Deschedule", func(t *testing.T) {
		_, err := r.DescheduleCronJob(job.JobKey())
		assert.NoError(t, err)
	})
}
func TestRealisClient_StartMaintenance(t *testing.T) {
	hosts := []string{"localhost"}

	_, _, err := r.StartMaintenance(hosts...)
	assert.NoError(t, err, "unable to start maintenance")

	// Monitor change to DRAINING and DRAINED mode
	hostResults, err := monitor.HostMaintenance(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_SCHEDULED},
		1,
		50)
	assert.Equal(t, map[string]bool{"localhost": true}, hostResults)
	assert.NoError(t, err)

	_, _, err = r.EndMaintenance(hosts...)
	assert.NoError(t, err)

	// Monitor change to DRAINING and DRAINED mode
	_, err = monitor.HostMaintenance(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
		5,
		10)
	assert.NoError(t, err)
}

func TestRealisClient_DrainHosts(t *testing.T) {
	hosts := []string{"localhost"}

	t.Run("DrainHosts", func(t *testing.T) {
		_, _, err := r.DrainHosts(hosts...)
		assert.NoError(t, err, "unable to drain host")
	})

	t.Run("MonitorTransitionToDrained", func(t *testing.T) {
		// Monitor change to DRAINING and DRAINED mode
		hostResults, err := monitor.HostMaintenance(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			1,
			50)
		assert.Equal(t, map[string]bool{"localhost": true}, hostResults)
		assert.NoError(t, err)
	})

	t.Run("MonitorNonExistentHost", func(t *testing.T) {
		// Monitor change to DRAINING and DRAINED mode
		hostResults, err := monitor.HostMaintenance(
			append(hosts, "IMAGINARY_HOST"),
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			1,
			1)

		// Assert monitor returned an error that was not nil, and also a list of the non-transitioned hosts
		assert.Error(t, err)
		assert.Equal(t, map[string]bool{"localhost": true, "IMAGINARY_HOST": false}, hostResults)
	})

	t.Run("EndMaintenance", func(t *testing.T) {
		_, _, err := r.EndMaintenance(hosts...)
		assert.NoError(t, err)

		// Monitor change to DRAINING and DRAINED mode
		_, err = monitor.HostMaintenance(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
			5,
			10)
		assert.NoError(t, err)
	})

}

func TestRealisClient_SLADrainHosts(t *testing.T) {
	hosts := []string{"localhost"}
	policy := aurora.SlaPolicy{PercentageSlaPolicy: &aurora.PercentageSlaPolicy{Percentage: 50.0}}

	_, err := r.SLADrainHosts(&policy, 30, hosts...)
	assert.NoError(t, err, "unable to drain host with SLA policy")

	// Monitor change to DRAINING and DRAINED mode
	hostResults, err := monitor.HostMaintenance(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
		1,
		50)
	assert.Equal(t, map[string]bool{"localhost": true}, hostResults)
	assert.NoError(t, err)

	_, _, err = r.EndMaintenance(hosts...)
	assert.NoError(t, err)

	// Monitor change to DRAINING and DRAINED mode
	_, err = monitor.HostMaintenance(
		hosts,
		[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
		5,
		10)
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

	_, err := r.CreateJob(job)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	threadCount := 20
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {

		// Launch multiple monitors that will poll every second
		go func() {
			defer wg.Done()

			// Test Schedule status monitor for terminal state and timing out after 30 seconds
			success, err := monitor.ScheduleStatus(job.JobKey(), job.GetInstanceCount(), realis.LiveStates, 1, 30)
			assert.False(t, success)
			assert.Error(t, err)

			_, err = r.KillJob(job.JobKey())
			assert.NoError(t, err)
		}()
	}

	wg.Wait()
}

// Test setting and getting the quota
func TestRealisClient_Quota(t *testing.T) {
	var resp *aurora.Response
	var err error

	cpu := 3.5
	ram := int64(20480)
	disk := int64(10240)

	t.Run("Set", func(t *testing.T) {
		resp, err = r.SetQuota("vagrant", &cpu, &ram, &disk)
		assert.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		// Test GetQuota based on previously set values
		var result *aurora.GetQuotaResult_
		resp, err = r.GetQuota("vagrant")
		if resp.GetResult_() != nil {
			result = resp.GetResult_().GetQuotaResult_
		}
		assert.NoError(t, err)

		for _, res := range result.Quota.GetResources() {
			switch true {
			case res.DiskMb != nil:
				assert.Equal(t, disk, *res.DiskMb)
			case res.NumCpus != nil:
				assert.Equal(t, cpu, *res.NumCpus)
			case res.RamMb != nil:
				assert.Equal(t, ram, *res.RamMb)
			}
		}
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
	err = r.ForceExplicitTaskReconciliation(thrift.Int32Ptr(32))
	assert.NoError(t, err)
}

func TestRealisClient_PartitionPolicy(t *testing.T) {

	role := "vagrant"
	job := realis.NewJob().
		Environment("prod").
		Role(role).
		Name("create_thermos_job_partition_policy_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData(string(thermosPayload)).
		CPU(.5).
		RAM(64).
		Disk(100).
		IsService(true).
		InstanceCount(2).
		PartitionPolicy(&aurora.PartitionPolicy{Reschedule: true, DelaySecs: thrift.Int64Ptr(30)})

	settings := realis.NewUpdateSettings()
	settings.UpdateGroupSize = 2

	_, result, err := r.CreateService(job, settings)
	assert.NoError(t, err)

	var ok bool
	var mErr error

	if ok, mErr = monitor.JobUpdate(*result.GetKey(), 5, 180); !ok || mErr != nil {
		// Update may already be in a terminal state so don't check for error
		_, err := r.AbortJobUpdate(*result.GetKey(), "Monitor timed out.")
		assert.NoError(t, err)
	}

	// Clean up after finishing test
	r.KillJob(job.JobKey())
}

func TestAuroraJob_UpdateSlaPolicy(t *testing.T) {

	tests := []struct {
		name string
		args aurora.SlaPolicy
	}{
		{
			"create_service_with_sla_count_policy_test",
			aurora.SlaPolicy{CountSlaPolicy: &aurora.CountSlaPolicy{Count: 1, DurationSecs: 15}},
		},
		{
			"create_service_with_sla_percentage_policy_test",
			aurora.SlaPolicy{PercentageSlaPolicy: &aurora.PercentageSlaPolicy{Percentage: 0.25, DurationSecs: 15}},
		},
		{
			"create_service_with_sla_coordinator_policy_test",
			aurora.SlaPolicy{CoordinatorSlaPolicy: &aurora.CoordinatorSlaPolicy{
				CoordinatorUrl: "http://localhost/endpoint", StatusKey: "aurora_test"}},
		},
	}
	role := "vagrant"

	_, err := r.SetQuota(role, thrift.Float64Ptr(6.0), thrift.Int64Ptr(1024), thrift.Int64Ptr(1024))
	assert.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Create a single job
			job := realis.NewJob().
				Environment("prod").
				Role(role).
				Name(tt.name).
				ExecutorName(aurora.AURORA_EXECUTOR_NAME).
				ExecutorData(string(thermosPayload)).
				CPU(.01).
				RAM(2).
				Disk(5).
				InstanceCount(4).
				IsService(true).
				SlaPolicy(&tt.args).
				Tier("preferred")

			settings := realis.NewUpdateSettings()
			settings.UpdateGroupSize = 2
			settings.MinWaitInInstanceRunningMs = 5 * 1000

			_, result, err := r.CreateService(job, settings)
			assert.NoError(t, err)
			assert.NotNil(t, result)

			var ok bool
			var mErr error

			if ok, mErr = monitor.JobUpdate(*result.GetKey(), 5, 240); !ok || mErr != nil {
				// Update may already be in a terminal state so don't check for error
				_, err := r.AbortJobUpdate(*result.GetKey(), "Monitor timed out.")

				_, err = r.KillJob(job.JobKey())

				assert.NoError(t, err)
			}
			assert.True(t, ok)
			assert.NoError(t, mErr)

			// Kill task test task after confirming it came up fine
			_, err = r.KillJob(job.JobKey())

			assert.NoError(t, err)
		})
	}
}

func TestAuroraURLValidator(t *testing.T) {

}
