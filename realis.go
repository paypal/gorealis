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

// Package realis provides the ability to use Thrift API to communicate with Apache Aurora.
package realis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/paypal/gorealis/v2/gen-go/apache/aurora"
	"github.com/paypal/gorealis/v2/response"
	"github.com/pkg/errors"
)

const VERSION = "2.0.1"

type Client struct {
	config         *clientConfig
	client         *aurora.AuroraSchedulerManagerClient
	readonlyClient *aurora.ReadOnlySchedulerClient
	adminClient    *aurora.AuroraAdminClient
	logger         LevelLogger
	lock           *sync.Mutex
	debug          bool
	transport      thrift.TTransport
}

type clientConfig struct {
	username, password    string
	url                   string
	timeout               time.Duration
	transportProtocol     TransportProtocol
	cluster               *Cluster
	backoff               Backoff
	transport             thrift.TTransport
	protoFactory          thrift.TProtocolFactory
	logger                *LevelLogger
	insecureSkipVerify    bool
	certsPath             string
	clientKey, clientCert string
	options               []ClientOption
	debug                 bool
	trace                 bool
	zkOptions             []ZKOpt
	failOnPermanentErrors bool
}

var defaultBackoff = Backoff{
	Steps:    3,
	Duration: 10 * time.Second,
	Factor:   5.0,
	Jitter:   0.1,
}

type TransportProtocol int

const (
	unsetProtocol TransportProtocol = iota
	jsonProtocol
	binaryProtocol
)

type ClientOption func(*clientConfig)

// clientConfig sets for options in clientConfig.
func BasicAuth(username, password string) ClientOption {
	return func(config *clientConfig) {
		config.username = username
		config.password = password
	}
}

func SchedulerUrl(url string) ClientOption {
	return func(config *clientConfig) {
		config.url = url
	}
}

func Timeout(timeout time.Duration) ClientOption {
	return func(config *clientConfig) {
		config.timeout = timeout
	}
}

func ZKCluster(cluster *Cluster) ClientOption {
	return func(config *clientConfig) {
		config.cluster = cluster
	}
}

func ZKUrl(url string) ClientOption {

	opts := []ZKOpt{ZKEndpoints(strings.Split(url, ",")...), ZKPath("/aurora/scheduler")}

	return func(config *clientConfig) {
		if config.zkOptions == nil {
			config.zkOptions = opts
		} else {
			config.zkOptions = append(config.zkOptions, opts...)
		}
	}
}

func ThriftJSON() ClientOption {
	return func(config *clientConfig) {
		config.transportProtocol = jsonProtocol
	}
}

func ThriftBinary() ClientOption {
	return func(config *clientConfig) {
		config.transportProtocol = binaryProtocol
	}
}

func BackOff(b Backoff) ClientOption {
	return func(config *clientConfig) {
		config.backoff = b
	}
}

func InsecureSkipVerify(InsecureSkipVerify bool) ClientOption {
	return func(config *clientConfig) {
		config.insecureSkipVerify = InsecureSkipVerify
	}
}

func CertsPath(certspath string) ClientOption {
	return func(config *clientConfig) {
		config.certsPath = certspath
	}
}

func ClientCerts(clientKey, clientCert string) ClientOption {
	return func(config *clientConfig) {
		config.clientKey, config.clientCert = clientKey, clientCert
	}
}

// Use this option if you'd like to override default settings for connecting to Zookeeper.
// See zk.go for what is possible to set as an option.
func ZookeeperOptions(opts ...ZKOpt) ClientOption {
	return func(config *clientConfig) {
		config.zkOptions = opts
	}
}

// Using the word set to avoid name collision with Interface.
func SetLogger(l Logger) ClientOption {
	return func(config *clientConfig) {
		config.logger = &LevelLogger{Logger: l}
	}
}

// Enable debug statements.
func Debug() ClientOption {
	return func(config *clientConfig) {
		config.debug = true
	}
}

// Enable trace statements.
func Trace() ClientOption {
	return func(config *clientConfig) {
		config.trace = true
	}
}

// FailOnPermanentErrors - If the client encounters a connection error the standard library
// considers permanent, stop retrying and return an error to the user.
func FailOnPermanentErrors() ClientOption {
	return func(config *clientConfig) {
		config.failOnPermanentErrors = true
	}
}

func newTJSONTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating realis")
	}

	httpTrans, ok := (trans).(*thrift.THttpClient)
	if !ok {
		return nil, errors.Wrap(err, "transport does not contain a thrift client")
	}

	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)
	return trans, err
}

func newTBinTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating realis")
	}

	httpTrans, ok := (trans).(*thrift.THttpClient)
	if !ok {
		return nil, errors.Wrap(err, "transport does not contain a thrift client")
	}

	httpTrans.DelHeader("Content-Type") // Workaround for using thrift HttpPostClient
	httpTrans.SetHeader("Accept", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("Content-Type", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)

	return trans, err
}

// This client implementation uses a retry mechanism for all Thrift Calls.
// It will retry all calls which result in a temporary failure as well as calls that fail due to an EOF
// being returned by the http client. Most permanent failures are now being caught by the thriftCallWithRetries
// function and not being retried but there may be corner cases not yet handled.
func NewClient(options ...ClientOption) (*Client, error) {
	config := &clientConfig{}

	// Default configs
	config.timeout = 10 * time.Second
	config.backoff = defaultBackoff
	config.logger = &LevelLogger{Logger: log.New(os.Stdout, "realis: ", log.Ltime|log.Ldate|log.LUTC)}

	// Save options to recreate client if a connection error happens
	config.options = options

	// Override default configs where necessary
	for _, opt := range options {
		opt(config)
	}

	// TODO(rdelvalle): Move this logic to it's own function to make initialization code easier to read.

	// Set a sane logger based upon configuration passed by the user
	if config.logger == nil {
		if config.debug || config.trace {
			config.logger = &LevelLogger{Logger: log.New(os.Stdout, "realis: ", log.Ltime|log.Ldate|log.LUTC)}
		} else {
			config.logger = &LevelLogger{Logger: NoopLogger{}}
		}
	}

	// Note, by this point, a LevelLogger should have been created.
	config.logger.EnableDebug(config.debug)
	config.logger.EnableTrace(config.trace)

	config.logger.DebugPrintln("Number of options applied to clientConfig: ", len(options))

	var url string
	var err error

	// Find the leader using custom Zookeeper options if options are provided
	if config.zkOptions != nil {
		url, err = LeaderFromZKOpts(config.zkOptions...)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "unable to determine leader from zookeeper"))
		}
		config.logger.Println("Scheduler URL from ZK: ", url)
	} else if config.cluster != nil {
		// Determine how to get information to connect to the scheduler.
		// Prioritize getting leader from ZK over using a direct URL.
		url, err = LeaderFromZK(*config.cluster)
		// If ZK is configured, throw an error if the leader is unable to be determined
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "unable to determine leader from zookeeper"))
		}
		config.logger.Println("Scheduler URL from ZK: ", url)
	} else if config.url != "" {
		url = config.url
		config.logger.Println("Scheduler URL: ", url)
	} else {
		return nil, errors.New("incomplete Options -- url, cluster.json, or Zookeeper address required")
	}

	url, err = validateAuroraAddress(url)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create realis object, invalid url")
	}

	switch config.transportProtocol {
	case binaryProtocol:
		trans, err := newTBinTransport(url, config.timeout, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
	case jsonProtocol:
		fallthrough
	default:
		trans, err := newTJSONTransport(url, config.timeout, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTJSONProtocolFactory()
	}

	config.logger.Printf("gorealis clientConfig url: %+v\n", url)

	// Adding Basic Authentication.
	if config.username != "" && config.password != "" {
		httpTrans, ok := (config.transport).(*thrift.THttpClient)
		if !ok {
			return nil, errors.New("transport provided does not contain an THttpClient")
		}
		httpTrans.SetHeader("Authorization", "Basic "+basicAuth(config.username, config.password))
	}

	return &Client{
		config:         config,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(config.transport, config.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(config.transport, config.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(config.transport, config.protoFactory),
		// We initialize logger this way to allow any logger which satisfies the Logger interface
		logger:    LevelLogger{Logger: config.logger, debug: config.debug, trace: config.trace},
		lock:      &sync.Mutex{},
		transport: config.transport,
	}, nil
}

func GetCerts(certPath string) (*x509.CertPool, error) {
	globalRootCAs := x509.NewCertPool()
	caFiles, err := ioutil.ReadDir(certPath)
	if err != nil {
		return nil, err
	}
	for _, cert := range caFiles {
		caPathFile := filepath.Join(certPath, cert.Name())
		caCert, err := ioutil.ReadFile(caPathFile)
		if err != nil {
			return nil, err
		}
		globalRootCAs.AppendCertsFromPEM(caCert)
	}
	return globalRootCAs, nil
}

// Creates a default Thrift Transport object for communications in gorealis using an HTTP Post Client
func defaultTTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	var transport http.Transport

	if config != nil {
		tlsConfig := &tls.Config{}
		if config.insecureSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		if config.certsPath != "" {
			rootCAs, err := GetCerts(config.certsPath)
			if err != nil {
				config.logger.Println("error occurred couldn't fetch certs")
				return nil, err
			}
			tlsConfig.RootCAs = rootCAs
		}
		if config.clientKey != "" && config.clientCert == "" {
			return nil, fmt.Errorf("have to provide both client key,cert. Only client key provided ")
		}
		if config.clientKey == "" && config.clientCert != "" {
			return nil, fmt.Errorf("have to provide both client key,cert. Only client cert provided ")
		}
		if config.clientKey != "" && config.clientCert != "" {
			cert, err := tls.LoadX509KeyPair(config.clientCert, config.clientKey)
			if err != nil {
				config.logger.Println("error occurred loading client certs and keys")
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		transport.TLSClientConfig = tlsConfig
	}

	trans, err := thrift.NewTHttpClientWithOptions(url,
		thrift.THttpClientOptions{
			Client: &http.Client{
				Timeout:   timeout,
				Transport: &transport,
			},
		})

	if err != nil {
		return nil, errors.Wrap(err, "error creating transport")
	}

	if err := trans.Open(); err != nil {
		return nil, errors.Wrapf(err, "error opening connection to %s", url)
	}

	return trans, nil
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func (c *Client) ReestablishConn() error {
	// Close existing connection
	c.logger.Println("Re-establishing Connection to Aurora")
	c.Close()

	c.lock.Lock()
	defer c.lock.Unlock()

	// Recreate connection from scratch using original options
	newRealis, err := NewClient(c.config.options...)
	if err != nil {
		// This could be a temporary network hiccup
		return NewTemporaryError(err)
	}

	// If we are able to successfully re-connect, make receiver
	// point to newly established connections.
	c.config = newRealis.config
	c.client = newRealis.client
	c.readonlyClient = newRealis.readonlyClient
	c.adminClient = newRealis.adminClient
	c.logger = newRealis.logger

	return nil
}

// Releases resources associated with the realis client.
func (c *Client) Close() {

	c.lock.Lock()
	defer c.lock.Unlock()

	c.transport.Close()
}

// Uses predefined set of states to retrieve a set of active jobs in Apache Aurora.
func (c *Client) GetInstanceIds(key aurora.JobKey, states []aurora.ScheduleStatus) ([]int32, error) {
	taskQ := &aurora.TaskQuery{
		Role:        &key.Role,
		Environment: &key.Environment,
		JobName:     &key.Name,
		Statuses:    states,
	}

	c.logger.DebugPrintf("GetTasksWithoutConfigs Thrift Payload: %+v\n", taskQ)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksWithoutConfigs(context.TODO(), taskQ)
	})

	// If we encountered an error we couldn't recover from by retrying, return an error to the user
	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for active IDs")
	}

	// Construct instance id map to stay in line with thrift's representation of sets
	tasks := response.ScheduleStatusResult(resp).GetTasks()
	jobInstanceIds := make([]int32, 0, len(tasks))
	for _, task := range tasks {
		jobInstanceIds = append(jobInstanceIds, task.GetAssignedTask().GetInstanceId())
	}
	return jobInstanceIds, nil

}

func (c *Client) GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.GetJobUpdateSummariesResult_, error) {
	c.logger.DebugPrintf("GetJobUpdateSummaries Thrift Payload: %+v\n", jobUpdateQuery)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobUpdateSummaries(context.TODO(), jobUpdateQuery)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error getting job update summaries from Aurora Scheduler")
	}

	return resp.GetResult_().GetGetJobUpdateSummariesResult_(), nil
}

func (c *Client) GetJobs(role string) (*aurora.GetJobsResult_, error) {

	var result *aurora.GetJobsResult_

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobs(context.TODO(), role)
	})

	if retryErr != nil {
		return result, errors.Wrap(retryErr, "error getting Jobs from Aurora Scheduler")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetJobsResult_
	}

	return result, nil
}

// Kill specific instances of a job. Returns true, nil if a task was actually killed as a result of this API call.
// Returns false, nil if no tasks were killed as a result of this call but there was no error making the call.
func (c *Client) KillInstances(key aurora.JobKey, instances ...int32) (bool, error) {
	c.logger.DebugPrintf("KillTasks Thrift Payload: %+v %v\n", key, instances)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.KillTasks(context.TODO(), &key, instances, "")
	})

	if retryErr != nil {
		return false, errors.Wrap(retryErr, "error sending Kill command to Aurora Scheduler")
	}

	if len(resp.GetDetails()) > 0 {
		c.logger.Println("KillTasks was called but no tasks killed as a result.")
		return false, nil
	} else {
		return true, nil
	}

}

func (c *Client) RealisConfig() *clientConfig {
	return c.config
}

// Sends a kill message to the scheduler for all active tasks under a job.
func (c *Client) KillJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("KillTasks Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		// Giving the KillTasks thrift call an empty set tells the Aurora scheduler to kill all active shards
		return c.client.KillTasks(context.TODO(), &key, nil, "")
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Kill command to Aurora Scheduler")
	}
	return nil
}

// Sends a create job message to the scheduler with a specific job configuration.
// Although this API is able to create service jobs, it is better to use CreateService instead
// as that API uses the update thrift call which has a few extra features available.
// Use this API to create ad-hoc jobs.
func (c *Client) CreateJob(auroraJob *AuroraJob) error {
	// If no thermos configuration has been set this will result in a NOOP
	err := auroraJob.BuildThermosPayload()

	c.logger.DebugPrintf("CreateJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	if err != nil {
		return errors.Wrap(err, "unable to create Thermos payload")
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.CreateJob(context.TODO(), auroraJob.JobConfig())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Create command to Aurora Scheduler")
	}

	return nil
}

// This API uses an update thrift call to create the services giving a few more robust features.
func (c *Client) CreateService(update *JobUpdate) (*aurora.StartJobUpdateResult_, error) {
	updateResult, err := c.StartJobUpdate(update, "")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create service")
	}

	return updateResult, err
}

func (c *Client) ScheduleCronJob(auroraJob *AuroraJob) error {
	// If no thermos configuration has been set this will result in a NOOP
	err := auroraJob.BuildThermosPayload()

	c.logger.DebugPrintf("ScheduleCronJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	if err != nil {
		return errors.Wrap(err, "Unable to create Thermos payload")
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.ScheduleCronJob(context.TODO(), auroraJob.JobConfig())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Cron AuroraJob Schedule message to Aurora Scheduler")
	}
	return nil
}

func (c *Client) DescheduleCronJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("DescheduleCronJob Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.DescheduleCronJob(context.TODO(), &key)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Cron AuroraJob De-schedule message to Aurora Scheduler")

	}
	return nil

}

func (c *Client) StartCronJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("StartCronJob Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.StartCronJob(context.TODO(), &key)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Start Cron AuroraJob  message to Aurora Scheduler")
	}
	return nil

}

// Restarts specific instances specified
func (c *Client) RestartInstances(key aurora.JobKey, instances ...int32) error {
	c.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instances)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.RestartShards(context.TODO(), &key, instances)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Restart command to Aurora Scheduler")
	}
	return nil
}

// Restarts all active tasks under a job configuration.
func (c *Client) RestartJob(key aurora.JobKey) error {

	instanceIds, err1 := c.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err1 != nil {
		return errors.Wrap(err1, "could not retrieve relevant task instance IDs")
	}

	c.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instanceIds)

	if len(instanceIds) > 0 {
		_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
			return c.client.RestartShards(context.TODO(), &key, instanceIds)
		})

		if retryErr != nil {
			return errors.Wrap(retryErr, "error sending Restart command to Aurora Scheduler")
		}

		return nil
	} else {
		return errors.New("no tasks in the Active state")
	}
}

// Update all tasks under a job configuration. Currently gorealis doesn't support for canary deployments.
func (c *Client) StartJobUpdate(updateJob *JobUpdate, message string) (*aurora.StartJobUpdateResult_, error) {

	if err := updateJob.BuildThermosPayload(); err != nil {
		return nil, errors.New("unable to generate the proper Thermos executor payload")
	}

	c.logger.DebugPrintf("StartJobUpdate Thrift Payload: %+v %v\n", updateJob, message)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.StartJobUpdate(nil, updateJob.request, message)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error sending StartJobUpdate command to Aurora Scheduler")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetStartJobUpdateResult_() != nil {
		return resp.GetResult_().GetStartJobUpdateResult_(), nil
	}

	return nil, errors.New("thrift error: Field in response is nil unexpectedly.")
}

// Abort AuroraJob Update on Aurora. Requires the updateId which can be obtained on the Aurora web UI.
func (c *Client) AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("AbortJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.AbortJobUpdate(context.TODO(), &updateKey, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending AbortJobUpdate command to Aurora Scheduler")
	}
	return nil
}

// Pause AuroraJob Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) PauseJobUpdate(updateKey *aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("PauseJobUpdate Thrift Payload: %+v %v\n", updateKey, message)
	// Thrift uses pointers for optional fields when generating Go code. To guarantee
	// immutability of the JobUpdateKey, perform a deep copy and store it locally.
	updateKeyLocal := &aurora.JobUpdateKey{
		Job: &aurora.JobKey{
			Role:        updateKey.Job.GetRole(),
			Environment: updateKey.Job.GetEnvironment(),
			Name:        updateKey.Job.GetName(),
		},
		ID: updateKey.GetID(),
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.PauseJobUpdate(nil, updateKeyLocal, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending PauseJobUpdate command to Aurora Scheduler")
	}

	// Make this call synchronous by  blocking until it job has successfully transitioned to aborted
	_, err := c.MonitorJobUpdateStatus(*updateKeyLocal,
		[]aurora.JobUpdateStatus{aurora.JobUpdateStatus_ABORTED},
		time.Second*5,
		time.Minute)

	return err
}

// Resume Paused AuroraJob Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) ResumeJobUpdate(updateKey *aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("ResumeJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.ResumeJobUpdate(context.TODO(), updateKey, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending ResumeJobUpdate command to Aurora Scheduler")
	}

	return nil
}

// Pulse AuroraJob Update on Aurora. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) PulseJobUpdate(updateKey *aurora.JobUpdateKey) (aurora.JobUpdatePulseStatus, error) {

	c.logger.DebugPrintf("PulseJobUpdate Thrift Payload: %+v\n", updateKey)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.PulseJobUpdate(context.TODO(), updateKey)
	})

	if retryErr != nil {
		return aurora.JobUpdatePulseStatus(0), errors.Wrap(retryErr, "error sending PulseJobUpdate command to Aurora Scheduler")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetPulseJobUpdateResult_() != nil {
		return resp.GetResult_().GetPulseJobUpdateResult_().GetStatus(), nil
	} else {
		return aurora.JobUpdatePulseStatus(0), errors.New("thrift error, field was nil unexpectedly")
	}

}

// Scale up the number of instances under a job configuration using the configuration for specific
// instance to scale up.
func (c *Client) AddInstances(instKey aurora.InstanceKey, count int32) error {

	c.logger.DebugPrintf("AddInstances Thrift Payload: %+v %v\n", instKey, count)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.AddInstances(context.TODO(), &instKey, count)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending AddInstances command to Aurora Scheduler")
	}
	return nil

}

// Scale down the number of instances under a job configuration using the configuration of a specific instance
// Instances with a higher instance ID will be removed first. For example, if our instance ID list is [0,1,2,3]
// and we want to remove 2 instances, 2 and 3 will always be picked.
func (c *Client) RemoveInstances(key aurora.JobKey, count int) error {
	instanceIds, err := c.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err != nil {
		return errors.Wrap(err, "removeInstances: Could not retrieve relevant instance IDs")
	}
	if len(instanceIds) < count {
		return errors.Errorf("insufficient active instances available for killing: "+
			" Instances to be killed %d Active instances %d", count, len(instanceIds))
	}

	// Sort instanceIds in decreasing order
	sort.Slice(instanceIds, func(i, j int) bool {
		return instanceIds[i] > instanceIds[j]
	})

	// Get the last count instance ids to kill
	instanceIds = instanceIds[:count]
	killed, err := c.KillInstances(key, instanceIds...)

	if !killed {
		return errors.New("flex down was not able to reduce the number of instances running.")
	}

	return nil
}

// Get information about task including a fully hydrated task configuration object
func (c *Client) GetTaskStatus(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error) {

	c.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksStatus(context.TODO(), query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for task status")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

// Get pending reason
func (c *Client) GetPendingReason(query *aurora.TaskQuery) ([]*aurora.PendingReason, error) {

	c.logger.DebugPrintf("GetPendingReason Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetPendingReason(context.TODO(), query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for pending Reasons")
	}

	var result []*aurora.PendingReason

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetGetPendingReasonResult_().GetReasons()
	}

	return result, nil
}

// Get information about task including without a task configuration object
func (c *Client) GetTasksWithoutConfigs(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error) {

	c.logger.DebugPrintf("GetTasksWithoutConfigs Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksWithoutConfigs(context.TODO(), query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for task status without configs")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil

}

// Get the task configuration from the aurora scheduler for a job
func (c *Client) FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error) {

	ids := []int32{instKey.GetInstanceId()}

	taskQ := &aurora.TaskQuery{
		Role:        &instKey.JobKey.Role,
		Environment: &instKey.JobKey.Environment,
		JobName:     &instKey.JobKey.Name,
		InstanceIds: ids,
		Statuses:    aurora.ACTIVE_STATES,
	}

	c.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", taskQ)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksStatus(context.TODO(), taskQ)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for task configuration")
	}

	tasks := response.ScheduleStatusResult(resp).GetTasks()

	if len(tasks) == 0 {
		return nil, errors.Errorf("instance %d for jobkey %s/%s/%s doesn't exist",
			instKey.InstanceId,
			instKey.JobKey.Environment,
			instKey.JobKey.Role,
			instKey.JobKey.Name)
	}

	// Currently, instance 0 is always picked..
	return tasks[0].AssignedTask.Task, nil
}

func (c *Client) JobUpdateDetails(updateQuery aurora.JobUpdateQuery) ([]*aurora.JobUpdateDetails, error) {

	c.logger.DebugPrintf("GetJobUpdateDetails Thrift Payload: %+v\n", updateQuery)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetJobUpdateDetails(context.TODO(), &updateQuery)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to get job update details")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetGetJobUpdateDetailsResult_() != nil {
		return resp.GetResult_().GetGetJobUpdateDetailsResult_().GetDetailsList(), nil
	} else {
		return nil, errors.New("unknown Thrift error, field is nil.")
	}
}

func (c *Client) RollbackJobUpdate(key aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("RollbackJobUpdate Thrift Payload: %+v %v\n", key, message)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.RollbackJobUpdate(context.TODO(), &key, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to roll back job update")
	}
	return nil
}
