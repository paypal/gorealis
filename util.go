package realis

import (
	"crypto/x509"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

const apiPath = "/api"

// ActiveStates - States a task may be in when active.
var ActiveStates = make(map[aurora.ScheduleStatus]bool)

// SlaveAssignedStates - States a task may be in when it has already been assigned to a Mesos agent.
var SlaveAssignedStates = make(map[aurora.ScheduleStatus]bool)

// LiveStates - States a task may be in when it is live (e.g. able to take traffic)
var LiveStates = make(map[aurora.ScheduleStatus]bool)

// TerminalStates - Set of states a task may not transition away from.
var TerminalStates = make(map[aurora.ScheduleStatus]bool)

// ActiveJobUpdateStates - States a Job Update may be in where it is considered active.
var ActiveJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)

// TerminalJobUpdateStates returns a slice containing all the terminal states an update may end up in.
// This is a function in order to avoid having a slice that can be accidentally mutated.
func TerminalUpdateStates() []aurora.JobUpdateStatus {
	return []aurora.JobUpdateStatus{
		aurora.JobUpdateStatus_ROLLED_FORWARD,
		aurora.JobUpdateStatus_ROLLED_BACK,
		aurora.JobUpdateStatus_ABORTED,
		aurora.JobUpdateStatus_ERROR,
		aurora.JobUpdateStatus_FAILED,
	}
}

// AwaitingPulseJobUpdateStates - States a job update may be in where it is waiting for a pulse.
var AwaitingPulseJobUpdateStates = make(map[aurora.JobUpdateStatus]bool)

func init() {
	for _, status := range aurora.ACTIVE_STATES {
		ActiveStates[status] = true
	}

	for _, status := range aurora.SLAVE_ASSIGNED_STATES {
		SlaveAssignedStates[status] = true
	}

	for _, status := range aurora.LIVE_STATES {
		LiveStates[status] = true
	}

	for _, status := range aurora.TERMINAL_STATES {
		TerminalStates[status] = true
	}

	for _, status := range aurora.ACTIVE_JOB_UPDATE_STATES {
		ActiveJobUpdateStates[status] = true
	}
	for _, status := range aurora.AWAITNG_PULSE_JOB_UPDATE_STATES {
		AwaitingPulseJobUpdateStates[status] = true
	}
}

// createCertPool will attempt to load certificates into a certificate pool from a given directory.
// Only files with an extension contained in the extension map are considered.
// This function ignores any files that cannot be read successfully or cannot be added to the certPool
// successfully.
func createCertPool(path string, extensions map[string]struct{}) (*x509.CertPool, error) {
	_, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load certificates")
	}

	caFiles, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	loadedCerts := 0
	for _, cert := range caFiles {
		// Skip directories
		if cert.IsDir() {
			continue
		}

		// Skip any files that do not contain the right extension
		if _, ok := extensions[filepath.Ext(cert.Name())]; !ok {
			continue
		}

		pem, err := ioutil.ReadFile(filepath.Join(path, cert.Name()))
		if err != nil {
			continue
		}

		if certPool.AppendCertsFromPEM(pem) {
			loadedCerts++
		}
	}
	if loadedCerts == 0 {
		return nil, errors.New("no certificates were able to be successfully loaded")
	}
	return certPool, nil
}

func validateAuroraURL(location string) (string, error) {

	// If no protocol defined, assume http
	if !strings.Contains(location, "://") {
		location = "http://" + location
	}

	u, err := url.Parse(location)

	if err != nil {
		return "", errors.Wrap(err, "error parsing url")
	}

	// If no path provided assume /api
	if u.Path == "" {
		u.Path = "/api"
	}

	// If no port provided, assume default 8081
	if u.Port() == "" {
		u.Host = u.Host + ":8081"
	}

	if !(u.Scheme == "http" || u.Scheme == "https") {
		return "", errors.Errorf("only protocols http and https are supported %v\n", u.Scheme)
	}

	// This could theoretically be elsewhere but we'll be strict for the sake of simplicity
	if u.Path != apiPath {
		return "", errors.Errorf("expected /api path %v\n", u.Path)
	}

	return u.String(), nil
}

func calculateCurrentBatch(updatingInstances int32, batchSizes []int32) int {
	for i, size := range batchSizes {
		updatingInstances -= size
		if updatingInstances <= 0 {
			return i
		}
	}

	// Overflow batches
	batchCount := len(batchSizes) - 1
	lastBatchIndex := len(batchSizes) - 1
	batchCount += int(updatingInstances / batchSizes[lastBatchIndex])

	if updatingInstances%batchSizes[lastBatchIndex] != 0 {
		batchCount++
	}
	return batchCount
}
