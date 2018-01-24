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
	"net/http"
	"net/http/cookiejar"
	"path/filepath"
	"time"

	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
)

const VERSION = "1.1.0"

type Realis interface {
	AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) (*aurora.Response, error)
	AddInstances(instKey aurora.InstanceKey, count int32) (*aurora.Response, error)
	CreateJob(auroraJob Job) (*aurora.Response, error)
	CreateService(auroraJob Job, settings UpdateSettings) (*aurora.Response, *aurora.StartJobUpdateResult_, error)
	DescheduleCronJob(key *aurora.JobKey) (*aurora.Response, error)
	FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error)
	GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error)
	GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.Response, error)
	GetTaskStatus(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error)
	GetTasksWithoutConfigs(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error)
	JobUpdateDetails(updateQuery aurora.JobUpdateQuery) (*aurora.Response, error)
	KillJob(key *aurora.JobKey) (*aurora.Response, error)
	KillInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error)
	RemoveInstances(key *aurora.JobKey, count int32) (*aurora.Response, error)
	RestartInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error)
	RestartJob(key *aurora.JobKey) (*aurora.Response, error)
	RollbackJobUpdate(key aurora.JobUpdateKey, message string) (*aurora.Response, error)
	ScheduleCronJob(auroraJob Job) (*aurora.Response, error)
	StartJobUpdate(updateJob *UpdateJob, message string) (*aurora.Response, error)
	StartCronJob(key *aurora.JobKey) (*aurora.Response, error)
	// TODO: Remove this method and make it private to avoid race conditions
	ReestablishConn() error
	RealisConfig() *RealisConfig
	Close()

	// Admin functions
	DrainHosts(hosts ...string) (*aurora.Response, *aurora.DrainHostsResult_, error)
	EndMaintenance(hosts ...string) (*aurora.Response, *aurora.EndMaintenanceResult_, error)
	MaintenanceStatus(hosts ...string) (*aurora.Response, *aurora.MaintenanceStatusResult_, error)
}

type realisClient struct {
	config         *RealisConfig
	client         *aurora.AuroraSchedulerManagerClient
	readonlyClient *aurora.ReadOnlySchedulerClient
	adminClient    *aurora.AuroraAdminClient
	logger         Logger
	lock           sync.Mutex
}

type RealisConfig struct {
	username, password          string
	url                         string
	timeoutms                   int
	binTransport, jsonTransport bool
	cluster                     *Cluster
	backoff                     *Backoff
	transport                   thrift.TTransport
	protoFactory                thrift.TProtocolFactory
	logger                      Logger
	InsecureSkipVerify          bool
	certspath                   string
	clientkey, clientcert       string
	options                     []ClientOption
}

type Backoff struct {
	Duration time.Duration // the base duration
	Factor   float64       // Duration is multipled by factor each iteration
	Jitter   float64       // The amount of jitter applied each iteration
	Steps    int           // Exit with error after this many steps
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
	return func(config *RealisConfig) {
		config.cluster = GetDefaultClusterFromZKUrl(url)
	}
}

func Retries(backoff *Backoff) ClientOption {
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

func BackOff(b *Backoff) ClientOption {
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

// Using the word set to avoid name collision with Interface
func SetLogger(l Logger) ClientOption {
	return func(config *RealisConfig) {
		config.logger = l
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

func NewRealisClient(options ...ClientOption) (Realis, error) {
	config := &RealisConfig{}

	// Default configs
	config.timeoutms = 10000
	config.backoff = &defaultBackoff
	config.logger = NoopLogger{}
	config.options = options

	// Override default configs where necessary
	for _, opt := range options {
		opt(config)
	}

	config.logger.Println("Number of options applied to config: ", len(options))

	//Set default Transport to JSON if needed.
	if !config.jsonTransport && !config.binTransport {
		config.jsonTransport = true
	}

	var url string
	var err error

	// Determine how to get information to connect to the scheduler.
	// Prioritize getting leader from ZK over using a direct URL.
	if config.cluster != nil {
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
		return nil, errors.New("Incomplete Options -- url or cluster required")
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

	//Basic Authentication.
	if config.username != "" && config.password != "" {
		AddBasicAuth(config, config.username, config.password)
	}

	return &realisClient{
		config:         config,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(config.transport, config.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(config.transport, config.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(config.transport, config.protoFactory),
		logger:         config.logger}, nil

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
			rootCAs, err := GetCerts("examples/certs")
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
	httpTrans.SetHeader("User-Agent", "GoRealis v"+VERSION)

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
	httpTrans.SetHeader("User-Agent", "GoRealis v"+VERSION)

	return &RealisConfig{transport: trans, protoFactory: thrift.NewTBinaryProtocolFactoryDefault()}, nil

}

// Helper function to add basic authorization needed to communicate with Apache Aurora.
func AddBasicAuth(config *RealisConfig, username string, password string) {
	config.username = username
	config.password = password
	httpTrans := (config.transport).(*thrift.THttpClient)
	httpTrans.SetHeader("Authorization", "Basic "+basicAuth(username, password))
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

type auroraThriftCall func() (resp *aurora.Response, err error)

// Takes a Thrift API function call and returns response and error.
// If Error from the API call is retryable, the functions re-establishes the connection with Aurora by
// using the same configuration used by the original client. Locks usage of and changes to client connection in order
// to make realis sessions thread safe.
func (r *realisClient) thriftCallHelper(auroraCall auroraThriftCall) (*aurora.Response, error) {
	// Only allow one go-routine make use or modify the thrift client connection
	r.lock.Lock()
	defer r.lock.Unlock()
	resp, cliErr := auroraCall()

	if cliErr != nil {
		// Re-establish conn returns a temporary error or nil
		// as we can always retry to connect to the scheduler.
		retryConnErr := r.ReestablishConn()

		return resp, retryConnErr
	}

	if resp == nil {
		return nil, errors.New("Response is nil")
	}

	if resp.GetResponseCode() == aurora.ResponseCode_ERROR_TRANSIENT {
		return resp, NewTemporaryError(errors.New("Aurora scheduler temporarily unavailable"))
	}

	if resp.GetResponseCode() != aurora.ResponseCode_OK {
		return nil, errors.New(response.CombineMessage(resp))
	}

	return resp, nil
}

func (r *realisClient) ReestablishConn() error {
	// Close existing connection
	r.logger.Println("Re-establishing Connection to Aurora")
	r.Close()

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
	r.client.Transport.Close()
	r.readonlyClient.Transport.Close()
	r.adminClient.Transport.Close()
}

// Uses predefined set of states to retrieve a set of active jobs in Apache Aurora.
func (r *realisClient) GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error) {
	taskQ := &aurora.TaskQuery{
		Role:        key.Role,
		Environment: key.Environment,
		JobName:     key.Name,
		Statuses:    states,
	}

	var resp *aurora.Response
	var clientErr error
	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {

		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.GetTasksWithoutConfigs(taskQ)
		})

		// Pass error directly to backoff which makes exceptions for temporary errors
		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	// If we encountered an error we couldn't recover from by retrying, return an error to the user
	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error querying Aurora Scheduler for active IDs")
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
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.readonlyClient.GetJobUpdateSummaries(jobUpdateQuery)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error getting job update summaries from Aurora Scheduler")
	}

	return resp, nil
}

// Kill specific instances of a job.
func (r *realisClient) KillInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error) {

	instanceIds := make(map[int32]bool)
	var resp *aurora.Response
	var clientErr error

	for _, instId := range instances {
		instanceIds[instId] = true
	}

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.KillTasks(key, instanceIds, "")
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Kill command to Aurora Scheduler")
	}
	return resp, nil
}

func (r *realisClient) RealisConfig() *RealisConfig {
	return r.config
}

// Sends a kill message to the scheduler for all active tasks under a job.
func (r *realisClient) KillJob(key *aurora.JobKey) (*aurora.Response, error) {
	var clientErr, err error
	var resp *aurora.Response

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			// Giving the KillTasks thrift call an empty set tells the Aurora scheduler to kill all active shards
			return r.client.KillTasks(key, nil, "")
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(err, retryErr.Error()+": Error sending Kill command to Aurora Scheduler")
	}
	return resp, nil
}

// Sends a create job message to the scheduler with a specific job configuration.
// Although this API is able to create service jobs, it is better to use CreateService instead
// as that API uses the update thrift call which has a few extra features available.
// Use this API to create ad-hoc jobs.
func (r *realisClient) CreateJob(auroraJob Job) (*aurora.Response, error) {
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.CreateJob(auroraJob.JobConfig())
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Create command to Aurora Scheduler")
	}
	return resp, nil
}

// This API uses an update thrift call to create the services giving a few more robust features.
func (r *realisClient) CreateService(auroraJob Job, settings UpdateSettings) (*aurora.Response, *aurora.StartJobUpdateResult_, error) {
	// Create a new job update object and ship it to the StartJobUpdate api
	update := NewUpdateJob(auroraJob.TaskConfig(), &settings.settings)
	update.InstanceCount(auroraJob.GetInstanceCount())
	update.BatchSize(auroraJob.GetInstanceCount())

	resp, err := r.StartJobUpdate(update, "")
	if err != nil {
		return resp, nil, errors.Wrap(err, "unable to create service")
	}

	if resp != nil && resp.GetResult_() != nil {
		return resp, resp.GetResult_().GetStartJobUpdateResult_(), nil
	}

	return resp, nil, errors.New("results object is nil")
}

func (r *realisClient) ScheduleCronJob(auroraJob Job) (*aurora.Response, error) {
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.ScheduleCronJob(auroraJob.JobConfig())
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Cron Job Schedule message to Aurora Scheduler")
	}
	return resp, nil
}

func (r *realisClient) DescheduleCronJob(key *aurora.JobKey) (*aurora.Response, error) {

	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.DescheduleCronJob(key)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Cron Job De-schedule message to Aurora Scheduler")
	}
	return resp, nil

}

func (r *realisClient) StartCronJob(key *aurora.JobKey) (*aurora.Response, error) {
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.StartCronJob(key)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Start Cron Job  message to Aurora Scheduler")
	}
	return resp, nil

}

// Restarts specific instances specified
func (r *realisClient) RestartInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error) {
	instanceIds := make(map[int32]bool)

	for _, instId := range instances {
		instanceIds[instId] = true
	}
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.RestartShards(key, instanceIds)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Restart command to Aurora Scheduler")
	}
	return resp, nil
}

// Restarts all active tasks under a job configuration.
func (r *realisClient) RestartJob(key *aurora.JobKey) (*aurora.Response, error) {

	instanceIds, err1 := r.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err1 != nil {
		return nil, errors.Wrap(err1, "Could not retrieve relevant task instance IDs")
	}
	var resp *aurora.Response
	var clientErr error
	if len(instanceIds) > 0 {
		retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
			resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
				return r.client.RestartShards(key, instanceIds)
			})

			if clientErr != nil {
				return false, clientErr
			}

			return true, nil
		})

		if retryErr != nil {
			return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending Restart command to Aurora Scheduler")
		}

		return resp, nil
	} else {
		return nil, errors.New("No tasks in the Active state")
	}
}

// Update all tasks under a job configuration. Currently gorealis doesn't support for canary deployments.
func (r *realisClient) StartJobUpdate(updateJob *UpdateJob, message string) (*aurora.Response, error) {

	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.StartJobUpdate(updateJob.req, message)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending StartJobUpdate command to Aurora Scheduler")
	}
	return resp, nil
}

// Abort Job Update on Aurora. Requires the updateId which can be obtained on the Aurora web UI.
func (r *realisClient) AbortJobUpdate(
	updateKey aurora.JobUpdateKey,
	message string) (*aurora.Response, error) {
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.AbortJobUpdate(&updateKey, message)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending AbortJobUpdate command to Aurora Scheduler")
	}
	return resp, nil
}

// Scale up the number of instances under a job configuration using the configuration for specific
// instance to scale up.
func (r *realisClient) AddInstances(instKey aurora.InstanceKey, count int32) (*aurora.Response, error) {

	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.AddInstances(&instKey, count)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error sending AddInstances command to Aurora Scheduler")
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

	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.GetTasksStatus(query)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error querying Aurora Scheduler for task status")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

// Get information about task including without a task configuration object
func (r *realisClient) GetTasksWithoutConfigs(query *aurora.TaskQuery) (tasks []*aurora.ScheduledTask, e error) {
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.GetTasksWithoutConfigs(query)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error querying Aurora Scheduler for task status without configs")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil

}

// Get the task configuration from the aurora scheduler for a job
func (r *realisClient) FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error) {

	ids := make(map[int32]bool)

	ids[instKey.InstanceId] = true
	taskQ := &aurora.TaskQuery{
		Role:        instKey.JobKey.Role,
		Environment: instKey.JobKey.Environment,
		JobName:     instKey.JobKey.Name,
		InstanceIds: ids,
		Statuses:    aurora.ACTIVE_STATES,
	}

	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.GetTasksStatus(taskQ)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Error querying Aurora Scheduler for task configuration")
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

	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.GetJobUpdateDetails(&updateQuery)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Unable to get job update details")
	}
	return resp, nil

}

func (r *realisClient) RollbackJobUpdate(key aurora.JobUpdateKey, message string) (*aurora.Response, error) {
	var resp *aurora.Response
	var clientErr error

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.client.RollbackJobUpdate(&key, message)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if retryErr != nil {
		return nil, errors.Wrap(clientErr, retryErr.Error()+": Unable to roll back job update")
	}
	return resp, nil
}

// Set a list of nodes to DRAINING. This means nothing will be able to be scheduled on them and any existing
// tasks will be killed and re-scheduled elsewhere in the cluster. Tasks from DRAINING nodes are not guaranteed
// to return to running unless there is enough capacity in the cluster to run them.
func (r *realisClient) DrainHosts(hosts ...string) (*aurora.Response, *aurora.DrainHostsResult_, error) {

	var resp *aurora.Response
	var result *aurora.DrainHostsResult_
	var clientErr error

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		drainList.HostNames[host] = true
	}

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.adminClient.DrainHosts(drainList)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if resp != nil && resp.GetResult_() != nil {
		result = resp.GetResult_().GetDrainHostsResult_()
	}

	if retryErr != nil {
		return resp, result, errors.Wrap(clientErr, retryErr.Error()+": Unable to recover connection")
	}

	return resp, result, nil
}

func (r *realisClient) EndMaintenance(hosts ...string) (*aurora.Response, *aurora.EndMaintenanceResult_, error) {

	var resp *aurora.Response
	var result *aurora.EndMaintenanceResult_
	var clientErr error

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to end maintenance on")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		hostList.HostNames[host] = true
	}

	retryErr := ExponentialBackoff(*r.config.backoff, func() (bool, error) {
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.adminClient.EndMaintenance(hostList)
		})

		if clientErr != nil {
			return false, clientErr
		}

		return true, nil
	})

	if resp != nil && resp.GetResult_() != nil {
		result = resp.GetResult_().GetEndMaintenanceResult_()
	}

	if retryErr != nil {
		return resp, result, errors.Wrap(clientErr, retryErr.Error()+": Unable to recover connection")
	}

	return resp, result, nil
}

func (r *realisClient) MaintenanceStatus(hosts ...string) (*aurora.Response, *aurora.MaintenanceStatusResult_, error) {

	var resp *aurora.Response
	var result *aurora.MaintenanceStatusResult_
	var clientErr error

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to get maintenance status from")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		hostList.HostNames[host] = true
	}

	retryErr := ExponentialBackoff(defaultBackoff, func() (bool, error) {

		// Make thrift call. If we encounter an error sending the call, attempt to reconnect
		// and continue trying to resend command until we run out of retries.
		resp, clientErr = r.thriftCallHelper(func() (*aurora.Response, error) {
			return r.adminClient.MaintenanceStatus(hostList)
		})

		if clientErr != nil {
			return false, clientErr
		}

		// Successful call
		return true, nil

	})

	if resp != nil && resp.GetResult_() != nil {
		result = resp.GetResult_().GetMaintenanceStatusResult_()
	}

	if retryErr != nil {
		return resp, result, errors.Wrap(clientErr, retryErr.Error()+": Unable to recover connection")
	}

	return resp, result, nil
}
