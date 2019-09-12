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
	"context"

	"github.com/paypal/gorealis/v2/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

// Set a list of nodes to DRAINING. This means nothing will be able to be scheduled on them and any existing
// tasks will be killed and re-scheduled elsewhere in the cluster. Tasks from DRAINING nodes are not guaranteed
// to return to running unless there is enough capacity in the cluster to run them.
func (c *Client) DrainHosts(hosts ...string) ([]*aurora.HostStatus, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = hosts

	c.logger.DebugPrintf("DrainHosts Thrift Payload: %v\n", drainList)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.DrainHosts(context.TODO(), drainList)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetDrainHostsResult_() != nil {
		return resp.GetResult_().GetDrainHostsResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("thrift error: Field in response is nil unexpectedly.")
	}
}

// Start SLA Aware Drain.
// defaultSlaPolicy is the fallback SlaPolicy to use if a task does not have an SlaPolicy.
// After timeoutSecs, tasks will be forcefully drained without checking SLA.
func (c *Client) SLADrainHosts(policy *aurora.SlaPolicy, timeout int64, hosts ...string) ([]*aurora.HostStatus, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = hosts

	c.logger.DebugPrintf("SLADrainHosts Thrift Payload: %v\n", drainList)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.SlaDrainHosts(context.TODO(), drainList, policy, timeout)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetDrainHostsResult_() != nil {
		return resp.GetResult_().GetDrainHostsResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("thrift error: Field in response is nil unexpectedly.")
	}
}

func (c *Client) StartMaintenance(hosts ...string) ([]*aurora.HostStatus, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to start maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	c.logger.DebugPrintf("StartMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.StartMaintenance(context.TODO(), hostList)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetStartMaintenanceResult_() != nil {
		return resp.GetResult_().GetStartMaintenanceResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("thrift error: Field in response is nil unexpectedly.")
	}
}

func (c *Client) EndMaintenance(hosts ...string) ([]*aurora.HostStatus, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to end maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	c.logger.DebugPrintf("EndMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.EndMaintenance(context.TODO(), hostList)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetEndMaintenanceResult_() != nil {
		return resp.GetResult_().GetEndMaintenanceResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("thrift error: Field in response is nil unexpectedly.")
	}

}

func (c *Client) MaintenanceStatus(hosts ...string) (*aurora.MaintenanceStatusResult_, error) {

	var result *aurora.MaintenanceStatusResult_

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to get maintenance status from")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	c.logger.DebugPrintf("MaintenanceStatus Thrift Payload: %v\n", hostList)

	// Make thrift call. If we encounter an error sending the call, attempt to reconnect
	// and continue trying to resend command until we run out of retries.
	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.MaintenanceStatus(context.TODO(), hostList)
	})

	if retryErr != nil {
		return result, errors.Wrap(retryErr, "unable to recover connection")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetMaintenanceStatusResult_()
	}

	return result, nil
}

// SetQuota sets a quota aggregate for the given role
// TODO(zircote) Currently investigating an error that is returned from thrift calls that include resources for `NamedPort` and `NumGpu`
func (c *Client) SetQuota(role string, cpu *float64, ramMb *int64, diskMb *int64) error {
	ramResource := aurora.NewResource()
	ramResource.RamMb = ramMb
	cpuResource := aurora.NewResource()
	cpuResource.NumCpus = cpu
	diskResource := aurora.NewResource()
	diskResource.DiskMb = diskMb

	quota := aurora.NewResourceAggregate()
	quota.Resources = []*aurora.Resource{ramResource, cpuResource, diskResource}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.SetQuota(context.TODO(), role, quota)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to set role quota")
	}
	return retryErr

}

// GetQuota returns the resource aggregate for the given role
func (c *Client) GetQuota(role string) (*aurora.GetQuotaResult_, error) {

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.GetQuota(context.TODO(), role)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to get role quota")
	}

	if resp.GetResult_() != nil {
		return resp.GetResult_().GetGetQuotaResult_(), nil
	} else {
		return nil, errors.New("thrift error: Field in response is nil unexpectedly.")
	}
}

// Force Aurora Scheduler to perform a snapshot and write to Mesos log
func (c *Client) Snapshot() error {

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.Snapshot(context.TODO())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to recover connection")
	}

	return nil
}

// Force Aurora Scheduler to write backup file to a file in the backup directory
func (c *Client) PerformBackup() error {

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.PerformBackup(context.TODO())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to recover connection")
	}

	return nil
}

// Force an Implicit reconciliation between Mesos and Aurora
func (c *Client) ForceImplicitTaskReconciliation() error {

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.TriggerImplicitTaskReconciliation(context.TODO())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to recover connection")
	}

	return nil
}

// Force an Explicit reconciliation between Mesos and Aurora
func (c *Client) ForceExplicitTaskReconciliation(batchSize *int32) error {

	if batchSize != nil && *batchSize < 1 {
		return errors.New("invalid batch size.")
	}
	settings := aurora.NewExplicitReconciliationSettings()

	settings.BatchSize = batchSize

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.adminClient.TriggerExplicitTaskReconciliation(context.TODO(), settings)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to recover connection")
	}

	return nil
}
