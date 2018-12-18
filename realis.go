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
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
)

const VERSION = "2.0.0"

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
	username, password          string
	url                         string
	timeout                     time.Duration
	binTransport, jsonTransport bool
	cluster                     *Cluster
	backoff                     Backoff
	transport                   thrift.TTransport
	protoFactory                thrift.TProtocolFactory
	logger                      *LevelLogger
	InsecureSkipVerify          bool
	certsPath                   string
	clientKey, clientCert       string
	options                     []ClientOption
	debug                       bool
	zkOptions                   []ZKOpt
}

var defaultBackoff = Backoff{
	Steps:    3,
	Duration: 10 * time.Second,
	Factor:   5.0,
	Jitter:   0.1,
}

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
		config.jsonTransport = true
	}
}

func ThriftBinary() ClientOption {
	return func(config *clientConfig) {
		config.binTransport = true
	}
}

func BackOff(b Backoff) ClientOption {
	return func(config *clientConfig) {
		config.backoff = b
	}
}

func InsecureSkipVerify(InsecureSkipVerify bool) ClientOption {
	return func(config *clientConfig) {
		config.InsecureSkipVerify = InsecureSkipVerify
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
		config.logger = &LevelLogger{l, false}
	}
}

// Enable debug statements.
func Debug() ClientOption {
	return func(config *clientConfig) {
		config.debug = true
	}
}

func newTJSONTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis")
	}
	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)
	return trans, err
}

func newTBinTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis")
	}
	httpTrans := (trans).(*thrift.THttpClient)
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
	config.logger = &LevelLogger{log.New(os.Stdout, "realis: ", log.Ltime|log.Ldate|log.LUTC), false}

	// Save options to recreate client if a connection error happens
	config.options = options

	// Override default configs where necessary
	for _, opt := range options {
		opt(config)
	}

	// TODO(rdelvalle): Move this logic to it's own function to make initialization code easier to read.

	// Turn off all logging (including debug)
	if config.logger == nil {
		config.logger = &LevelLogger{NoopLogger{}, false}
	}

	// Set a logger if debug has been set to true but no logger has been set
	if config.logger == nil && config.debug {
		config.logger = &LevelLogger{log.New(os.Stdout, "realis: ", log.Ltime|log.Ldate|log.LUTC), true}
	}

	// Note, by this point, a LevelLogger should have been created.
	config.logger.EnableDebug(config.debug)

	config.logger.DebugPrintln("Number of options applied to clientConfig: ", len(options))

	// Set default Transport to JSON if needed.
	if !config.jsonTransport && !config.binTransport {
		config.jsonTransport = true
	}

	var url string
	var err error

	// Find the leader using custom Zookeeper options if options are provided
	if config.zkOptions != nil {
		url, err = LeaderFromZKOpts(config.zkOptions...)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "LeaderFromZK error"))
		}
		config.logger.Println("Scheduler URL from ZK: ", url)
	} else if config.cluster != nil {
		// Determine how to get information to connect to the scheduler.
		// Prioritize getting leader from ZK over using a direct URL.
		url, err = LeaderFromZK(*config.cluster)
		// If ZK is configured, throw an error if the leader is unable to be determined
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "LeaderFromZK error"))
		}
		config.logger.Println("Scheduler URL from ZK: ", url)
	} else if config.url != "" {
		url = config.url
		config.logger.Println("Scheduler URL: ", url)
	} else {
		return nil, errors.New("Incomplete Options -- url, cluster.json, or Zookeeper address required")
	}

	url, err = validateAndPopulateAuroraURL(url)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create realis object, invalid url")
	}

	if config.jsonTransport {
		trans, err := newTJSONTransport(url, config.timeout, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "Error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTJSONProtocolFactory()

	} else if config.binTransport {
		trans, err := newTBinTransport(url, config.timeout, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "Error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}

	config.logger.Printf("gorealis clientConfig url: %+v\n", url)

	// Adding Basic Authentication.
	if config.username != "" && config.password != "" {
		httpTrans := (config.transport).(*thrift.THttpClient)
		httpTrans.SetHeader("Authorization", "Basic "+basicAuth(config.username, config.password))
	}

	return &Client{
		config:         config,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(config.transport, config.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(config.transport, config.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(config.transport, config.protoFactory),
		logger:         LevelLogger{config.logger, config.debug},
		lock:           &sync.Mutex{},
		transport:      config.transport,
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

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating Cookie Jar")
	}

	if config != nil {
		tlsConfig := &tls.Config{}
		if config.InsecureSkipVerify {
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
		thrift.THttpClientOptions{Client: &http.Client{Timeout: timeout, Transport: &transport, Jar: jar}})

	if err != nil {
		return nil, errors.Wrap(err, "Error creating transport")
	}

	if err := trans.Open(); err != nil {
		return nil, errors.Wrapf(err, "Error opening connection to %s", url)
	}

	return trans, nil
}

// Create a default configuration of the transport layer, requires a URL to test connection with.
// Uses HTTP Post as transport layer and Thrift JSON as the wire protocol by default.
func newDefaultConfig(url string, timeout time.Duration, config *clientConfig) (*clientConfig, error) {
	return newTJSONConfig(url, timeout, config)
}

// Creates a realis clientConfig object using HTTP Post and Thrift JSON protocol to communicate with Aurora.
func newTJSONConfig(url string, timeout time.Duration, config *clientConfig) (*clientConfig, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis clientConfig")
	}

	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)

	return &clientConfig{transport: trans, protoFactory: thrift.NewTJSONProtocolFactory()}, nil
}

// Creates a realis clientConfig clientConfig using HTTP Post and Thrift Binary protocol to communicate with Aurora.
func newTBinaryConfig(url string, timeout time.Duration, config *clientConfig) (*clientConfig, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis clientConfig")
	}

	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.DelHeader("Content-Type") // Workaround for using thrift HttpPostClient

	httpTrans.SetHeader("Accept", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("Content-Type", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)

	return &clientConfig{transport: trans, protoFactory: thrift.NewTBinaryProtocolFactoryDefault()}, nil

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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.GetTasksWithoutConfigs(nil, taskQ)
	})

	// If we encountered an error we couldn't recover from by retrying, return an error to the user
	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for active IDs")
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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobUpdateSummaries(nil, jobUpdateQuery)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error getting job update summaries from Aurora Scheduler")
	}

	return resp.GetResult_().GetGetJobUpdateSummariesResult_(), nil
}

func (c *Client) GetJobs(role string) (*aurora.GetJobsResult_, error) {

	var result *aurora.GetJobsResult_

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobs(nil, role)
	})

	if retryErr != nil {
		return result, errors.Wrap(retryErr, "Error getting Jobs from Aurora Scheduler")
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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.KillTasks(nil, &key, instances, "")
	})

	if retryErr != nil {
		return false, errors.Wrap(retryErr, "Error sending Kill command to Aurora Scheduler")
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

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		// Giving the KillTasks thrift call an empty set tells the Aurora scheduler to kill all active shards
		return c.client.KillTasks(nil, &key, nil, "")
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending Kill command to Aurora Scheduler")
	}
	return nil
}

// Sends a create job message to the scheduler with a specific job configuration.
// Although this API is able to create service jobs, it is better to use CreateService instead
// as that API uses the update thrift call which has a few extra features available.
// Use this API to create ad-hoc jobs.
func (c *Client) CreateJob(auroraJob *AuroraJob) error {

	c.logger.DebugPrintf("CreateJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.CreateJob(nil, auroraJob.JobConfig())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending Create command to Aurora Scheduler")
	}

	return nil
}

// This API uses an update thrift call to create the services giving a few more robust features.
func (c *Client) CreateService(update *JobUpdate) (*aurora.StartJobUpdateResult_, error) {
	updateResult, err := c.StartJobUpdate(update, "")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create service")
	}

	if updateResult != nil {
		return updateResult, nil
	}

	return nil, errors.New("results object is nil")
}

func (c *Client) ScheduleCronJob(auroraJob *AuroraJob) error {
	c.logger.DebugPrintf("ScheduleCronJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.ScheduleCronJob(nil, auroraJob.JobConfig())
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending Cron AuroraJob Schedule message to Aurora Scheduler")
	}
	return nil
}

func (c *Client) DescheduleCronJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("DescheduleCronJob Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.DescheduleCronJob(nil, &key)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending Cron AuroraJob De-schedule message to Aurora Scheduler")

	}
	return nil

}

func (c *Client) StartCronJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("StartCronJob Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.StartCronJob(nil, &key)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending Start Cron AuroraJob  message to Aurora Scheduler")
	}
	return nil

}

// Restarts specific instances specified
func (c *Client) RestartInstances(key aurora.JobKey, instances ...int32) error {
	c.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instances)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.RestartShards(nil, &key, instances)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending Restart command to Aurora Scheduler")
	}
	return nil
}

// Restarts all active tasks under a job configuration.
func (c *Client) RestartJob(key aurora.JobKey) error {

	instanceIds, err1 := c.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err1 != nil {
		return errors.Wrap(err1, "Could not retrieve relevant task instance IDs")
	}

	c.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instanceIds)

	if len(instanceIds) > 0 {
		_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
			return c.client.RestartShards(nil, &key, instanceIds)
		})

		if retryErr != nil {
			return errors.Wrap(retryErr, "Error sending Restart command to Aurora Scheduler")
		}

		return nil
	} else {
		return errors.New("No tasks in the Active state")
	}
}

// Update all tasks under a job configuration. Currently gorealis doesn't support for canary deployments.
func (c *Client) StartJobUpdate(updateJob *JobUpdate, message string) (*aurora.StartJobUpdateResult_, error) {

	c.logger.DebugPrintf("StartJobUpdate Thrift Payload: %+v %v\n", updateJob, message)

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.StartJobUpdate(nil, updateJob.request, message)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending StartJobUpdate command to Aurora Scheduler")
	}
	return resp.GetResult_().GetStartJobUpdateResult_(), nil
}

// Abort AuroraJob Update on Aurora. Requires the updateId which can be obtained on the Aurora web UI.
func (c *Client) AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("AbortJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.AbortJobUpdate(nil, &updateKey, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending AbortJobUpdate command to Aurora Scheduler")
	}
	return nil
}

// Pause AuroraJob Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) PauseJobUpdate(updateKey *aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("PauseJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.PauseJobUpdate(nil, updateKey, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending PauseJobUpdate command to Aurora Scheduler")
	}

	return nil
}

// Resume Paused AuroraJob Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) ResumeJobUpdate(updateKey *aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("ResumeJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.ResumeJobUpdate(nil, updateKey, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending ResumeJobUpdate command to Aurora Scheduler")
	}

	return nil
}

// Pulse AuroraJob Update on Aurora. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) PulseJobUpdate(updateKey *aurora.JobUpdateKey) (aurora.JobUpdatePulseStatus, error) {

	c.logger.DebugPrintf("PulseJobUpdate Thrift Payload: %+v\n", updateKey)

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.PulseJobUpdate(nil, updateKey)
	})

	if retryErr != nil {
		return aurora.JobUpdatePulseStatus(0), errors.Wrap(retryErr, "Error sending PulseJobUpdate command to Aurora Scheduler")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetPulseJobUpdateResult_() != nil {
		return resp.GetResult_().GetPulseJobUpdateResult_().GetStatus(), nil
	} else {
		return aurora.JobUpdatePulseStatus(0), errors.New("Thrift error, field was nil unexpectedly")
	}

}

// Scale up the number of instances under a job configuration using the configuration for specific
// instance to scale up.
func (c *Client) AddInstances(instKey aurora.InstanceKey, count int32) error {

	c.logger.DebugPrintf("AddInstances Thrift Payload: %+v %v\n", instKey, count)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.AddInstances(nil, &instKey, count)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Error sending AddInstances command to Aurora Scheduler")
	}
	return nil

}

// Scale down the number of instances under a job configuration using the configuration of a specific instance
// Instances with a higher instance ID will be removed first. For example, if our instance ID list is [0,1,2,3]
// and we want to remove 2 instances, 2 and 3 will always be picked.
func (c *Client) RemoveInstances(key aurora.JobKey, count int) error {
	instanceIds, err := c.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err != nil {
		return errors.Wrap(err, "RemoveInstances: Could not retrieve relevant instance IDs")
	}
	if len(instanceIds) < count {
		return errors.Errorf("Insufficient active instances available for killing: "+
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
		return errors.New("Flex down was not able to reduce the number of instances running.")
	}

	return nil
}

// Get information about task including a fully hydrated task configuration object
func (c *Client) GetTaskStatus(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error) {

	c.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.GetTasksStatus(nil, query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for task status")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

// Get pending reason
func (c *Client) GetPendingReason(query *aurora.TaskQuery) ([]*aurora.PendingReason, error) {

	c.logger.DebugPrintf("GetPendingReason Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.GetPendingReason(nil, query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for pending Reasons")
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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.GetTasksWithoutConfigs(nil, query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for task status without configs")
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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.GetTasksStatus(nil, taskQ)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for task configuration")
	}

	tasks := response.ScheduleStatusResult(resp).GetTasks()

	if len(tasks) == 0 {
		return nil, errors.Errorf("Instance %d for jobkey %s/%s/%s doesn't exist",
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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.GetJobUpdateDetails(nil, &updateQuery)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to get job update details")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetGetJobUpdateDetailsResult_() != nil {
		return resp.GetResult_().GetGetJobUpdateDetailsResult_().GetDetailsList(), nil
	} else {
		return nil, errors.New("Unknown Thrift error, field is nil.")
	}
}

func (c *Client) RollbackJobUpdate(key aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("RollbackJobUpdate Thrift Payload: %+v %v\n", key, message)

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.client.RollbackJobUpdate(nil, &key, message)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to roll back job update")
	}
	return nil
}

/* Admin functions */
// TODO(rdelvalle): Consider moving these functions to another interface. It would be a backwards incompatible change,
// but would add safety.

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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.DrainHosts(nil, drainList)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetDrainHostsResult_() != nil {
		return resp.GetResult_().GetDrainHostsResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("Thrift error: Field in response is nil unexpectedly.")
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

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.SlaDrainHosts(nil, drainList, policy, timeout)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetDrainHostsResult_() != nil {
		return resp.GetResult_().GetDrainHostsResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("Thrift error: Field in response is nil unexpectedly.")
	}
}

func (c *Client) StartMaintenance(hosts ...string) ([]*aurora.HostStatus, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to start maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	c.logger.DebugPrintf("StartMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.StartMaintenance(nil, hostList)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetStartMaintenanceResult_() != nil {
		return resp.GetResult_().GetStartMaintenanceResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("Thrift error: Field in response is nil unexpectedly.")
	}
}

func (c *Client) EndMaintenance(hosts ...string) ([]*aurora.HostStatus, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to end maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = hosts

	c.logger.DebugPrintf("EndMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.EndMaintenance(nil, hostList)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to recover connection")
	}

	if resp.GetResult_() != nil && resp.GetResult_().GetEndMaintenanceResult_() != nil {
		return resp.GetResult_().GetEndMaintenanceResult_().GetStatuses(), nil
	} else {
		return nil, errors.New("Thrift error: Field in response is nil unexpectedly.")
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
	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.MaintenanceStatus(nil, hostList)
	})

	if retryErr != nil {
		return result, errors.Wrap(retryErr, "Unable to recover connection")
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

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.SetQuota(nil, role, quota)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to set role quota")
	}
	return retryErr

}

// GetQuota returns the resource aggregate for the given role
func (c *Client) GetQuota(role string) (*aurora.GetQuotaResult_, error) {

	resp, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.GetQuota(nil, role)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to get role quota")
	}

	if resp.GetResult_() != nil {
		return resp.GetResult_().GetGetQuotaResult_(), nil
	} else {
		return nil, errors.New("Thrift error: Field in response is nil unexpectedly.")
	}
}

// Force Aurora Scheduler to perform a snapshot and write to Mesos log
func (c *Client) Snapshot() error {

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.Snapshot(nil)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

// Force Aurora Scheduler to write backup file to a file in the backup directory
func (c *Client) PerformBackup() error {

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.PerformBackup(nil)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

// Force an Implicit reconciliation between Mesos and Aurora
func (c *Client) ForceImplicitTaskReconciliation() error {

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.TriggerImplicitTaskReconciliation(nil)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

// Force an Explicit reconciliation between Mesos and Aurora
func (c *Client) ForceExplicitTaskReconciliation(batchSize *int32) error {

	if batchSize != nil && *batchSize < 1 {
		return errors.New("Invalid batch size.")
	}
	settings := aurora.NewExplicitReconciliationSettings()

	settings.BatchSize = batchSize

	_, retryErr := c.thriftCallWithRetries(func() (*aurora.Response, error) {
		return c.adminClient.TriggerExplicitTaskReconciliation(nil, settings)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}
