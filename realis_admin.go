package realis

import (
	"context"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

// TODO(rdelvalle): Consider moving these functions to another interface. It would be a backwards incompatible change,
// but would add safety.

// Set a list of nodes to DRAINING. This means nothing will be able to be scheduled on them and any existing
// tasks will be killed and re-scheduled elsewhere in the cluster. Tasks from DRAINING nodes are not guaranteed
// to return to running unless there is enough capacity in the cluster to run them.
func (r *realisClient) DrainHosts(hosts ...string) (*aurora.Response, *aurora.DrainHostsResult_, error) {

	var result *aurora.DrainHostsResult_

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = hosts

	r.logger.DebugPrintf("DrainHosts Thrift Payload: %v\n", drainList)

	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.DrainHosts(context.TODO(), drainList)
		})

	if retryErr != nil {
		return resp, result, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetDrainHostsResult_()
	}

	return resp, result, nil
}

// Start SLA Aware Drain.
// defaultSlaPolicy is the fallback SlaPolicy to use if a task does not have an SlaPolicy.
// After timeoutSecs, tasks will be forcefully drained without checking SLA.
func (r *realisClient) SLADrainHosts(
	policy *aurora.SlaPolicy,
	timeout int64,
	hosts ...string) (*aurora.DrainHostsResult_, error) {
	var result *aurora.DrainHostsResult_

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = hosts

	r.logger.DebugPrintf("SLADrainHosts Thrift Payload: %v\n", drainList)

	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.SlaDrainHosts(context.TODO(), drainList, policy, timeout)
		})

	if retryErr != nil {
		return result, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetDrainHostsResult_()
	}

	return result, nil
}

func (r *realisClient) StartMaintenance(hosts ...string) (*aurora.Response, *aurora.StartMaintenanceResult_, error) {

	var result *aurora.StartMaintenanceResult_

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to start maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	r.logger.DebugPrintf("StartMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.StartMaintenance(context.TODO(), hostList)
		})

	if retryErr != nil {
		return resp, result, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetStartMaintenanceResult_()
	}

	return resp, result, nil
}

func (r *realisClient) EndMaintenance(hosts ...string) (*aurora.Response, *aurora.EndMaintenanceResult_, error) {

	var result *aurora.EndMaintenanceResult_

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to end maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	r.logger.DebugPrintf("EndMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.EndMaintenance(context.TODO(), hostList)
		})

	if retryErr != nil {
		return resp, result, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetEndMaintenanceResult_()
	}

	return resp, result, nil
}

func (r *realisClient) MaintenanceStatus(hosts ...string) (*aurora.Response, *aurora.MaintenanceStatusResult_, error) {

	var result *aurora.MaintenanceStatusResult_

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to get maintenance status from")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	r.logger.DebugPrintf("MaintenanceStatus Thrift Payload: %v\n", hostList)

	// Make thrift call. If we encounter an error sending the call, attempt to reconnect
	// and continue trying to resend command until we run out of retries.
	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.MaintenanceStatus(context.TODO(), hostList)
		})

	if retryErr != nil {
		return resp, result, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetMaintenanceStatusResult_()
	}

	return resp, result, nil
}

// SetQuota sets a quota aggregate for the given role
// TODO(zircote) Currently investigating an error that is returned
// from thrift calls that include resources for `NamedPort` and `NumGpu`
func (r *realisClient) SetQuota(role string, cpu *float64, ramMb *int64, diskMb *int64) (*aurora.Response, error) {
	quota := &aurora.ResourceAggregate{
		Resources: []*aurora.Resource{{NumCpus: cpu}, {RamMb: ramMb}, {DiskMb: diskMb}},
	}

	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.SetQuota(context.TODO(), role, quota)
		})

	if retryErr != nil {
		return resp, errors.Wrap(retryErr, "Unable to set role quota")
	}
	return resp, retryErr

}

// GetQuota returns the resource aggregate for the given role
func (r *realisClient) GetQuota(role string) (*aurora.Response, error) {

	resp, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.GetQuota(context.TODO(), role)
		})

	if retryErr != nil {
		return resp, errors.Wrap(retryErr, "Unable to get role quota")
	}
	return resp, retryErr
}

// Force Aurora Scheduler to perform a snapshot and write to Mesos log
func (r *realisClient) Snapshot() error {

	_, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.Snapshot(context.TODO())
		})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

// Force Aurora Scheduler to write backup file to a file in the backup directory
func (r *realisClient) PerformBackup() error {

	_, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.PerformBackup(context.TODO())
		})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

func (r *realisClient) ForceImplicitTaskReconciliation() error {

	_, retryErr := r.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return r.adminClient.TriggerImplicitTaskReconciliation(context.TODO())
		})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

func (r *realisClient) ForceExplicitTaskReconciliation(batchSize *int32) error {

	if batchSize != nil && *batchSize < 1 {
		return errors.New("invalid batch size")
	}
	settings := aurora.NewExplicitReconciliationSettings()

	settings.BatchSize = batchSize

	_, retryErr := r.thriftCallWithRetries(false,
		func() (*aurora.Response, error) {
			return r.adminClient.TriggerExplicitTaskReconciliation(context.TODO(), settings)
		})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}
