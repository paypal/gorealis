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
	"strings"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
)

const VERSION = "1.3.0"

// TODO(rdelvalle): Move documentation to interface in order to make godoc look better/more accessible
type Realis interface {
	AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) (*aurora.Response, error)
	AddInstances(instKey aurora.InstanceKey, count int32) (*aurora.Response, error)
	CreateJob(auroraJob Job) (*aurora.Response, error)
	CreateService(auroraJob Job, settings *aurora.JobUpdateSettings) (*aurora.Response, *aurora.StartJobUpdateResult_, error)
	DescheduleCronJob(key *aurora.JobKey) (*aurora.Response, error)
	FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error)
	GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error)
	GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.Response, error)
	GetTaskStatus(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error)
	GetTasksWithoutConfigs(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error)
	GetJobs(role string) (*aurora.Response, *aurora.GetJobsResult_, error)
	GetPendingReason(query *aurora.TaskQuery) (pendingReasons []*aurora.PendingReason, e error)
	JobUpdateDetails(updateQuery aurora.JobUpdateQuery) (*aurora.Response, error)
	KillJob(key *aurora.JobKey) (*aurora.Response, error)
	KillInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error)
	RemoveInstances(key *aurora.JobKey, count int32) (*aurora.Response, error)
	RestartInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error)
	RestartJob(key *aurora.JobKey) (*aurora.Response, error)
	RollbackJobUpdate(key aurora.JobUpdateKey, message string) (*aurora.Response, error)
	ScheduleCronJob(auroraJob Job) (*aurora.Response, error)
	StartJobUpdate(updateJob *UpdateJob, message string) (*aurora.Response, error)

	PauseJobUpdate(key *aurora.JobUpdateKey, message string) (*aurora.Response, error)
	ResumeJobUpdate(key *aurora.JobUpdateKey, message string) (*aurora.Response, error)
	PulseJobUpdate(key *aurora.JobUpdateKey) (*aurora.Response, error)
	StartCronJob(key *aurora.JobKey) (*aurora.Response, error)
	// TODO: Remove this method and make it private to avoid race conditions
	ReestablishConn() error
	RealisConfig() *RealisConfig
	Close()

	// Admin functions
	DrainHosts(hosts ...string) (*aurora.Response, *aurora.DrainHostsResult_, error)
	SLADrainHosts(policy *aurora.SlaPolicy, timeout int64, hosts ...string) (*aurora.DrainHostsResult_, error)
	StartMaintenance(hosts ...string) (*aurora.Response, *aurora.StartMaintenanceResult_, error)
	EndMaintenance(hosts ...string) (*aurora.Response, *aurora.EndMaintenanceResult_, error)
	MaintenanceStatus(hosts ...string) (*aurora.Response, *aurora.MaintenanceStatusResult_, error)
	SetQuota(role string, cpu *float64, ram *int64, disk *int64) (*aurora.Response, error)
	GetQuota(role string) (*aurora.Response, error)
	Snapshot() error
	PerformBackup() error
	// Force an Implicit reconciliation between Mesos and Aurora
	ForceImplicitTaskReconciliation() error
	// Force an Explicit reconciliation between Mesos and Aurora
	ForceExplicitTaskReconciliation(batchSize *int32) error
}

type realisClient struct {
	config         *RealisConfig
	client         *aurora.AuroraSchedulerManagerClient
	readonlyClient *aurora.ReadOnlySchedulerClient
	adminClient    *aurora.AuroraAdminClient
	logger         LevelLogger
	lock           *sync.Mutex
	debug          bool
}

type RealisConfig struct {
	username, password          string
	url                         string
	timeoutms                   int
	binTransport, jsonTransport bool
	cluster                     *Cluster
	backoff                     Backoff
	transport                   thrift.TTransport
	protoFactory                thrift.TProtocolFactory
	logger                      *LevelLogger
	InsecureSkipVerify          bool
	certspath                   string
	clientkey, clientcert       string
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

type ClientOption func(*RealisConfig)

//Config sets for options in RealisConfig.
func BasicAuth(username, password string) ClientOption {
	return func(config *RealisConfig) {
		config.username = username
		config.password = password
	}
}

func SchedulerUrl(url string) ClientOption {
	return func(config *RealisConfig) {
		config.url = url
	}
}

func TimeoutMS(timeout int) ClientOption {
	return func(config *RealisConfig) {
		config.timeoutms = timeout
	}
}

func ZKCluster(cluster *Cluster) ClientOption {
	return func(config *RealisConfig) {
		config.cluster = cluster
	}
}

func ZKUrl(url string) ClientOption {

	opts := []ZKOpt{ZKEndpoints(strings.Split(url, ",")...), ZKPath("/aurora/scheduler")}

	return func(config *RealisConfig) {
		if config.zkOptions == nil {
			config.zkOptions = opts
		} else {
			config.zkOptions = append(config.zkOptions, opts...)
		}
	}
}

func Retries(backoff Backoff) ClientOption {
	return func(config *RealisConfig) {
		config.backoff = backoff
	}
}

func ThriftJSON() ClientOption {
	return func(config *RealisConfig) {
		config.jsonTransport = true
	}
}

func ThriftBinary() ClientOption {
	return func(config *RealisConfig) {
		config.binTransport = true
	}
}

func BackOff(b Backoff) ClientOption {
	return func(config *RealisConfig) {
		config.backoff = b
	}
}

func InsecureSkipVerify(InsecureSkipVerify bool) ClientOption {
	return func(config *RealisConfig) {
		config.InsecureSkipVerify = InsecureSkipVerify
	}
}

func Certspath(certspath string) ClientOption {
	return func(config *RealisConfig) {
		config.certspath = certspath
	}
}

func ClientCerts(clientKey, clientCert string) ClientOption {
	return func(config *RealisConfig) {
		config.clientkey, config.clientcert = clientKey, clientCert
	}
}

// Use this option if you'd like to override default settings for connecting to Zookeeper.
// See zk.go for what is possible to set as an option.
func ZookeeperOptions(opts ...ZKOpt) ClientOption {
	return func(config *RealisConfig) {
		config.zkOptions = opts
	}
}

// Using the word set to avoid name collision with Interface.
func SetLogger(l Logger) ClientOption {
	return func(config *RealisConfig) {
		config.logger = &LevelLogger{l, false}
	}
}

// Enable debug statements.
func Debug() ClientOption {
	return func(config *RealisConfig) {
		config.debug = true
	}
}

func newTJSONTransport(url string, timeout int, config *RealisConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis")
	}
	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "GoRealis v"+VERSION)
	return trans, err
}

func newTBinTransport(url string, timeout int, config *RealisConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis")
	}
	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.DelHeader("Content-Type") // Workaround for using thrift HttpPostClient
	httpTrans.SetHeader("Accept", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("Content-Type", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("User-Agent", "GoRealis v"+VERSION)

	return trans, err
}

// This client implementation of the realis interface uses a retry mechanism for all Thrift Calls.
// It will retry all calls which result in a temporary failure as well as calls that fail due to an EOF
// being returned by the http client. Most permanent failures are now being caught by the thriftCallWithRetries
// function and not being retried but there may be corner cases not yet handled.
func NewRealisClient(options ...ClientOption) (Realis, error) {
	config := &RealisConfig{}

	// Default configs
	config.timeoutms = 10000
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

	config.logger.DebugPrintln("Number of options applied to config: ", len(options))

	//Set default Transport to JSON if needed.
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

	if config.jsonTransport {
		trans, err := newTJSONTransport(url, config.timeoutms, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "Error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTJSONProtocolFactory()

	} else if config.binTransport {
		trans, err := newTBinTransport(url, config.timeoutms, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "Error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}

	config.logger.Printf("gorealis config url: %+v\n", url)

	// Adding Basic Authentication.
	if config.username != "" && config.password != "" {
		httpTrans := (config.transport).(*thrift.THttpClient)
		httpTrans.SetHeader("Authorization", "Basic "+basicAuth(config.username, config.password))
	}

	return &realisClient{
		config:         config,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(config.transport, config.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(config.transport, config.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(config.transport, config.protoFactory),
		logger:         LevelLogger{config.logger, config.debug},
		lock:           &sync.Mutex{}}, nil
}

func GetDefaultClusterFromZKUrl(zkurl string) *Cluster {
	return &Cluster{
		Name:          "defaultCluster",
		AuthMechanism: "UNAUTHENTICATED",
		ZK:            zkurl,
		SchedZKPath:   "/aurora/scheduler",
		AgentRunDir:   "latest",
		AgentRoot:     "/var/lib/mesos",
	}
}

func GetCerts(certpath string) (*x509.CertPool, error) {
	globalRootCAs := x509.NewCertPool()
	caFiles, err := ioutil.ReadDir(certpath)
	if err != nil {
		return nil, err
	}
	for _, cert := range caFiles {
		capathfile := filepath.Join(certpath, cert.Name())
		caCert, err := ioutil.ReadFile(capathfile)
		if err != nil {
			return nil, err
		}
		globalRootCAs.AppendCertsFromPEM(caCert)
	}
	return globalRootCAs, nil
}

// Creates a default Thrift Transport object for communications in gorealis using an HTTP Post Client
func defaultTTransport(urlstr string, timeoutms int, config *RealisConfig) (thrift.TTransport, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return &thrift.THttpClient{}, errors.Wrap(err, "Error creating Cookie Jar")
	}
	var transport http.Transport
	if config != nil {
		tlsConfig := &tls.Config{}
		if config.InsecureSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		if config.certspath != "" {
			rootCAs, err := GetCerts(config.certspath)
			if err != nil {
				config.logger.Println("error occured couldn't fetch certs")
				return nil, err
			}
			tlsConfig.RootCAs = rootCAs
		}
		if config.clientkey != "" && config.clientcert == "" {
			return nil, fmt.Errorf("have to provide both client key,cert. Only client key provided ")
		}
		if config.clientkey == "" && config.clientcert != "" {
			return nil, fmt.Errorf("have to provide both client key,cert. Only client cert provided ")
		}
		if config.clientkey != "" && config.clientcert != "" {
			cert, err := tls.LoadX509KeyPair(config.clientcert, config.clientkey)
			if err != nil {
				config.logger.Println("error occured loading client certs and keys")
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		transport.TLSClientConfig = tlsConfig
	}

	trans, err := thrift.NewTHttpPostClientWithOptions(urlstr+"/api",
		thrift.THttpClientOptions{Client: &http.Client{Timeout: time.Millisecond * time.Duration(timeoutms), Transport: &transport, Jar: jar}})

	if err != nil {
		return &thrift.THttpClient{}, errors.Wrap(err, "Error creating transport")
	}

	if err := trans.Open(); err != nil {
		return &thrift.THttpClient{}, errors.Wrapf(err, "Error opening connection to %s", urlstr)
	}

	return trans, nil
}

// Create a default configuration of the transport layer, requires a URL to test connection with.
// Uses HTTP Post as transport layer and Thrift JSON as the wire protocol by default.
func newDefaultConfig(url string, timeoutms int, config *RealisConfig) (*RealisConfig, error) {
	return newTJSONConfig(url, timeoutms, config)
}

// Creates a realis config object using HTTP Post and Thrift JSON protocol to communicate with Aurora.
func newTJSONConfig(url string, timeoutms int, config *RealisConfig) (*RealisConfig, error) {
	trans, err := defaultTTransport(url, timeoutms, config)
	if err != nil {
		return &RealisConfig{}, errors.Wrap(err, "Error creating realis config")
	}

	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)

	return &RealisConfig{transport: trans, protoFactory: thrift.NewTJSONProtocolFactory()}, nil
}

// Creates a realis config config using HTTP Post and Thrift Binary protocol to communicate with Aurora.
func newTBinaryConfig(url string, timeoutms int, config *RealisConfig) (*RealisConfig, error) {
	trans, err := defaultTTransport(url, timeoutms, config)
	if err != nil {
		return &RealisConfig{}, errors.Wrap(err, "Error creating realis config")
	}

	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.DelHeader("Content-Type") // Workaround for using thrift HttpPostClient

	httpTrans.SetHeader("Accept", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("Content-Type", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)

	return &RealisConfig{transport: trans, protoFactory: thrift.NewTBinaryProtocolFactoryDefault()}, nil

}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func (r *realisClient) ReestablishConn() error {
	// Close existing connection
	r.logger.Println("Re-establishing Connection to Aurora")
	r.Close()

	r.lock.Lock()
	defer r.lock.Unlock()

	// Recreate connection from scratch using original options
	newRealis, err := NewRealisClient(r.config.options...)
	if err != nil {
		// This could be a temporary network hiccup
		return NewTemporaryError(err)
	}

	// If we are able to successfully re-connect, make receiver
	// point to newly established connections.
	if newClient, ok := newRealis.(*realisClient); ok {
		r.config = newClient.config
		r.client = newClient.client
		r.readonlyClient = newClient.readonlyClient
		r.adminClient = newClient.adminClient
		r.logger = newClient.logger
	}

	return nil
}

// Releases resources associated with the realis client.
func (r *realisClient) Close() {

	r.lock.Lock()
	defer r.lock.Unlock()

	r.client.Transport.Close()
	r.readonlyClient.Transport.Close()
	r.adminClient.Transport.Close()
}

// Uses predefined set of states to retrieve a set of active jobs in Apache Aurora.
func (r *realisClient) GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error) {
	taskQ := &aurora.TaskQuery{
		Role:        &key.Role,
		Environment: &key.Environment,
		JobName:     &key.Name,
		Statuses:    states,
	}

	r.logger.DebugPrintf("GetTasksWithoutConfigs Thrift Payload: %+v\n", taskQ)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.GetTasksWithoutConfigs(taskQ)
	})

	// If we encountered an error we couldn't recover from by retrying, return an error to the user
	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for active IDs")
	}

	// Construct instance id map to stay in line with thrift's representation of sets
	tasks := response.ScheduleStatusResult(resp).GetTasks()
	jobInstanceIds := make(map[int32]bool)
	for _, task := range tasks {
		jobInstanceIds[task.GetAssignedTask().GetInstanceId()] = true
	}
	return jobInstanceIds, nil

}

func (r *realisClient) GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.Response, error) {
	r.logger.DebugPrintf("GetJobUpdateSummaries Thrift Payload: %+v\n", jobUpdateQuery)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.readonlyClient.GetJobUpdateSummaries(jobUpdateQuery)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error getting job update summaries from Aurora Scheduler")
	}

	return resp, nil
}

func (r *realisClient) GetJobs(role string) (*aurora.Response, *aurora.GetJobsResult_, error) {

	var result *aurora.GetJobsResult_

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.readonlyClient.GetJobs(role)
	})

	if retryErr != nil {
		return nil, result, errors.Wrap(retryErr, "Error getting Jobs from Aurora Scheduler")
	}

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetJobsResult_
	}

	return resp, result, nil
}

// Kill specific instances of a job.
func (r *realisClient) KillInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error) {
	r.logger.DebugPrintf("KillTasks Thrift Payload: %+v %v\n", key, instances)

	instanceIds := make(map[int32]bool)

	for _, instId := range instances {
		instanceIds[instId] = true
	}

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.KillTasks(key, instanceIds, "")
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending Kill command to Aurora Scheduler")
	}
	return resp, nil
}

func (r *realisClient) RealisConfig() *RealisConfig {
	return r.config
}

// Sends a kill message to the scheduler for all active tasks under a job.
func (r *realisClient) KillJob(key *aurora.JobKey) (*aurora.Response, error) {

	r.logger.DebugPrintf("KillTasks Thrift Payload: %+v\n", key)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		// Giving the KillTasks thrift call an empty set tells the Aurora scheduler to kill all active shards
		return r.client.KillTasks(key, nil, "")
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending Kill command to Aurora Scheduler")
	}
	return resp, nil
}

// Sends a create job message to the scheduler with a specific job configuration.
// Although this API is able to create service jobs, it is better to use CreateService instead
// as that API uses the update thrift call which has a few extra features available.
// Use this API to create ad-hoc jobs.
func (r *realisClient) CreateJob(auroraJob Job) (*aurora.Response, error) {

	r.logger.DebugPrintf("CreateJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.CreateJob(auroraJob.JobConfig())
	})

	if retryErr != nil {
		return resp, errors.Wrap(retryErr, "Error sending Create command to Aurora Scheduler")
	}
	return resp, nil
}

// This API uses an update thrift call to create the services giving a few more robust features.
func (r *realisClient) CreateService(auroraJob Job, settings *aurora.JobUpdateSettings) (*aurora.Response, *aurora.StartJobUpdateResult_, error) {
	// Create a new job update object and ship it to the StartJobUpdate api
	update := NewUpdateJob(auroraJob.TaskConfig(), settings)
	update.InstanceCount(auroraJob.GetInstanceCount())

	resp, err := r.StartJobUpdate(update, "")
	if err != nil {
		return resp, nil, errors.Wrap(err, "unable to create service")
	}

	if resp.GetResult_() != nil {
		return resp, resp.GetResult_().GetStartJobUpdateResult_(), nil
	}

	return nil, nil, errors.New("results object is nil")
}

func (r *realisClient) ScheduleCronJob(auroraJob Job) (*aurora.Response, error) {
	r.logger.DebugPrintf("ScheduleCronJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.ScheduleCronJob(auroraJob.JobConfig())
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending Cron Job Schedule message to Aurora Scheduler")
	}
	return resp, nil
}

func (r *realisClient) DescheduleCronJob(key *aurora.JobKey) (*aurora.Response, error) {

	r.logger.DebugPrintf("DescheduleCronJob Thrift Payload: %+v\n", key)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.DescheduleCronJob(key)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending Cron Job De-schedule message to Aurora Scheduler")

	}
	return resp, nil

}

func (r *realisClient) StartCronJob(key *aurora.JobKey) (*aurora.Response, error) {

	r.logger.DebugPrintf("StartCronJob Thrift Payload: %+v\n", key)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.StartCronJob(key)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending Start Cron Job  message to Aurora Scheduler")
	}
	return resp, nil

}

// Restarts specific instances specified
func (r *realisClient) RestartInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error) {
	r.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instances)

	instanceIds := make(map[int32]bool)

	for _, instId := range instances {
		instanceIds[instId] = true
	}

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.RestartShards(key, instanceIds)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending Restart command to Aurora Scheduler")
	}
	return resp, nil
}

// Restarts all active tasks under a job configuration.
func (r *realisClient) RestartJob(key *aurora.JobKey) (*aurora.Response, error) {

	instanceIds, err1 := r.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err1 != nil {
		return nil, errors.Wrap(err1, "Could not retrieve relevant task instance IDs")
	}

	r.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instanceIds)

	if len(instanceIds) > 0 {
		resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
			return r.client.RestartShards(key, instanceIds)
		})

		if retryErr != nil {
			return nil, errors.Wrap(retryErr, "Error sending Restart command to Aurora Scheduler")
		}

		return resp, nil
	} else {
		return nil, errors.New("No tasks in the Active state")
	}
}

// Update all tasks under a job configuration. Currently gorealis doesn't support for canary deployments.
func (r *realisClient) StartJobUpdate(updateJob *UpdateJob, message string) (*aurora.Response, error) {

	r.logger.DebugPrintf("StartJobUpdate Thrift Payload: %+v %v\n", updateJob, message)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.StartJobUpdate(updateJob.req, message)
	})

	if retryErr != nil {
		return resp, errors.Wrap(retryErr, "Error sending StartJobUpdate command to Aurora Scheduler")
	}
	return resp, nil
}

// Abort Job Update on Aurora. Requires the updateId which can be obtained on the Aurora web UI.
func (r *realisClient) AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) (*aurora.Response, error) {

	r.logger.DebugPrintf("AbortJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.AbortJobUpdate(&updateKey, message)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending AbortJobUpdate command to Aurora Scheduler")
	}
	return resp, nil
}

//Pause Job Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (r *realisClient) PauseJobUpdate(updateKey *aurora.JobUpdateKey, message string) (*aurora.Response, error) {

	r.logger.DebugPrintf("PauseJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.PauseJobUpdate(updateKey, message)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending PauseJobUpdate command to Aurora Scheduler")
	}

	return resp, nil
}

//Resume Paused Job Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (r *realisClient) ResumeJobUpdate(updateKey *aurora.JobUpdateKey, message string) (*aurora.Response, error) {

	r.logger.DebugPrintf("ResumeJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.ResumeJobUpdate(updateKey, message)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending ResumeJobUpdate command to Aurora Scheduler")
	}

	return resp, nil
}

//Pulse Job Update on Aurora. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (r *realisClient) PulseJobUpdate(updateKey *aurora.JobUpdateKey) (*aurora.Response, error) {

	r.logger.DebugPrintf("PulseJobUpdate Thrift Payload: %+v\n", updateKey)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.PulseJobUpdate(updateKey)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending PulseJobUpdate command to Aurora Scheduler")
	}

	return resp, nil
}

// Scale up the number of instances under a job configuration using the configuration for specific
// instance to scale up.
func (r *realisClient) AddInstances(instKey aurora.InstanceKey, count int32) (*aurora.Response, error) {

	r.logger.DebugPrintf("AddInstances Thrift Payload: %+v %v\n", instKey, count)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.AddInstances(&instKey, count)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error sending AddInstances command to Aurora Scheduler")
	}
	return resp, nil

}

//Scale down the number of instances under a job configuration using the configuration of a specific instance
func (r *realisClient) RemoveInstances(key *aurora.JobKey, count int32) (*aurora.Response, error) {
	instanceIds, err := r.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err != nil {
		return nil, errors.Wrap(err, "RemoveInstances: Could not retrieve relevant instance IDs")
	}
	if len(instanceIds) < int(count) {
		return nil, errors.New(fmt.Sprintf("RemoveInstances: No sufficient instances to Kill - "+
			"Instances to kill %d Total Instances %d", count, len(instanceIds)))
	}
	instanceList := make([]int32, count)
	i := 0
	for k := range instanceIds {
		instanceList[i] = k
		i += 1
		if i == int(count) {
			break
		}
	}
	return r.KillInstances(key, instanceList...)
}

// Get information about task including a fully hydrated task configuration object
func (r *realisClient) GetTaskStatus(query *aurora.TaskQuery) (tasks []*aurora.ScheduledTask, e error) {

	r.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", query)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.GetTasksStatus(query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for task status")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

// Get pending reason
func (r *realisClient) GetPendingReason(query *aurora.TaskQuery) (pendingReasons []*aurora.PendingReason, e error) {

	r.logger.DebugPrintf("GetPendingReason Thrift Payload: %+v\n", query)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.GetPendingReason(query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for pending Reasons")
	}

	var result map[*aurora.PendingReason]bool

	if resp.GetResult_() != nil {
		result = resp.GetResult_().GetGetPendingReasonResult_().GetReasons()
	}
	for reason := range result {
		pendingReasons = append(pendingReasons, reason)
	}
	return pendingReasons, nil
}

// Get information about task including without a task configuration object
func (r *realisClient) GetTasksWithoutConfigs(query *aurora.TaskQuery) (tasks []*aurora.ScheduledTask, e error) {

	r.logger.DebugPrintf("GetTasksWithoutConfigs Thrift Payload: %+v\n", query)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.GetTasksWithoutConfigs(query)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Error querying Aurora Scheduler for task status without configs")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil

}

// Get the task configuration from the aurora scheduler for a job
func (r *realisClient) FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error) {

	ids := make(map[int32]bool)

	ids[instKey.InstanceId] = true
	taskQ := &aurora.TaskQuery{
		Role:        &instKey.JobKey.Role,
		Environment: &instKey.JobKey.Environment,
		JobName:     &instKey.JobKey.Name,
		InstanceIds: ids,
		Statuses:    aurora.ACTIVE_STATES,
	}

	r.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", taskQ)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.GetTasksStatus(taskQ)
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

func (r *realisClient) JobUpdateDetails(updateQuery aurora.JobUpdateQuery) (*aurora.Response, error) {

	r.logger.DebugPrintf("GetJobUpdateDetails Thrift Payload: %+v\n", updateQuery)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.GetJobUpdateDetails(&updateQuery)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to get job update details")
	}
	return resp, nil

}

func (r *realisClient) RollbackJobUpdate(key aurora.JobUpdateKey, message string) (*aurora.Response, error) {

	r.logger.DebugPrintf("RollbackJobUpdate Thrift Payload: %+v %v\n", key, message)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.client.RollbackJobUpdate(&key, message)
	})

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "Unable to roll back job update")
	}
	return resp, nil
}

/* Admin functions */
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
	drainList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		drainList.HostNames[host] = true
	}

	r.logger.DebugPrintf("DrainHosts Thrift Payload: %v\n", drainList)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.DrainHosts(drainList)
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
func (r *realisClient) SLADrainHosts(policy *aurora.SlaPolicy, timeout int64, hosts ...string) (*aurora.DrainHostsResult_, error) {
	var result *aurora.DrainHostsResult_

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		drainList.HostNames[host] = true
	}

	r.logger.DebugPrintf("SLADrainHosts Thrift Payload: %v\n", drainList)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.SlaDrainHosts(drainList, policy, timeout)
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
	hostList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		hostList.HostNames[host] = true
	}

	r.logger.DebugPrintf("StartMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.StartMaintenance(hostList)
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
	hostList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		hostList.HostNames[host] = true
	}

	r.logger.DebugPrintf("EndMaintenance Thrift Payload: %v\n", hostList)

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.EndMaintenance(hostList)
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
	hostList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		hostList.HostNames[host] = true
	}

	r.logger.DebugPrintf("MaintenanceStatus Thrift Payload: %v\n", hostList)

	// Make thrift call. If we encounter an error sending the call, attempt to reconnect
	// and continue trying to resend command until we run out of retries.
	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.MaintenanceStatus(hostList)
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
// TODO(zircote) Currently investigating an error that is returned from thrift calls that include resources for `NamedPort` and `NumGpu`
func (r *realisClient) SetQuota(role string, cpu *float64, ramMb *int64, diskMb *int64) (*aurora.Response, error) {
	ram := aurora.NewResource()
	ram.RamMb = ramMb
	c := aurora.NewResource()
	c.NumCpus = cpu
	d := aurora.NewResource()
	d.DiskMb = diskMb
	quota := aurora.NewResourceAggregate()
	quota.Resources = make(map[*aurora.Resource]bool)
	quota.Resources[ram] = true
	quota.Resources[c] = true
	quota.Resources[d] = true
	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.SetQuota(role, quota)
	})

	if retryErr != nil {
		return resp, errors.Wrap(retryErr, "Unable to set role quota")
	}
	return resp, retryErr

}

// GetQuota returns the resource aggregate for the given role
func (r *realisClient) GetQuota(role string) (*aurora.Response, error) {

	resp, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.GetQuota(role)
	})

	if retryErr != nil {
		return resp, errors.Wrap(retryErr, "Unable to get role quota")
	}
	return resp, retryErr
}

// Force Aurora Scheduler to perform a snapshot and write to Mesos log
func (r *realisClient) Snapshot() error {

	_, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.Snapshot()
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

// Force Aurora Scheduler to write backup file to a file in the backup directory
func (r *realisClient) PerformBackup() error {

	_, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.PerformBackup()
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

func (r *realisClient) ForceImplicitTaskReconciliation() error {

	_, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.TriggerImplicitTaskReconciliation()
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}

func (r *realisClient) ForceExplicitTaskReconciliation(batchSize *int32) error {

	if batchSize != nil && *batchSize < 1 {
		return errors.New("Invalid batch size.")
	}
	settings := aurora.NewExplicitReconciliationSettings()

	settings.BatchSize = batchSize

	_, retryErr := r.thriftCallWithRetries(func() (*aurora.Response, error) {
		return r.adminClient.TriggerExplicitTaskReconciliation(settings)
	})

	if retryErr != nil {
		return errors.Wrap(retryErr, "Unable to recover connection")
	}

	return nil
}
