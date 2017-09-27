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
	"encoding/base64"
	"net/http"
	"net/http/cookiejar"
	"time"

	"fmt"

	"math/rand"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/pkg/errors"
	"github.com/rdelval/gorealis/gen-go/apache/aurora"
	"github.com/rdelval/gorealis/response"
)

const VERSION = "1.0.4"

type Realis interface {
	AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) (*aurora.Response, error)
	AddInstances(instKey aurora.InstanceKey, count int32) (*aurora.Response, error)
	RemoveInstances(key *aurora.JobKey, count int32) (*aurora.Response, error)
	CreateJob(auroraJob Job) (*aurora.Response, error)
	DescheduleCronJob(key *aurora.JobKey) (*aurora.Response, error)
	GetTaskStatus(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error)
	GetTasksWithoutConfigs(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error)
	FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error)
	GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error)
	JobUpdateDetails(updateQuery aurora.JobUpdateQuery) (*aurora.Response, error)
	KillJob(key *aurora.JobKey) (*aurora.Response, error)
	KillInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error)
	RestartInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error)
	RestartJob(key *aurora.JobKey) (*aurora.Response, error)
	RollbackJobUpdate(key aurora.JobUpdateKey, message string) (*aurora.Response, error)
	ScheduleCronJob(auroraJob Job) (*aurora.Response, error)
	StartJobUpdate(updateJob *UpdateJob, message string) (*aurora.Response, error)
	StartCronJob(key *aurora.JobKey) (*aurora.Response, error)
	GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.Response, error)
	ReestablishConn() error
	RealisConfig() *RealisConfig
	Close()

	// Admin functions
	DrainHosts(hosts ...string) (*aurora.Response, *aurora.DrainHostsResult_, error)
	EndMaintenance(hosts ...string) (*aurora.Response, *aurora.EndMaintenanceResult_, error)
}

type realisClient struct {
	config         *RealisConfig
	client         *aurora.AuroraSchedulerManagerClient
	readonlyClient *aurora.ReadOnlySchedulerClient
	adminClient    *aurora.AuroraAdminClient
}

type option func(*RealisConfig)

//Config sets for options in RealisConfig.
func BasicAuth(username, password string) option {

	return func(config *RealisConfig) {
		config.username = username
		config.password = password
	}
}

func SchedulerUrl(url string) option {
	return func(config *RealisConfig) {
		config.url = url
	}
}

func TimeoutMS(timeout int) option {
	return func(config *RealisConfig) {
		config.timeoutms = timeout
	}
}

func ZKCluster(cluster *Cluster) option {
	return func(config *RealisConfig) {
		config.cluster = cluster
	}
}

func ZKUrl(url string) option {
	return func(config *RealisConfig) {
		config.cluster = GetDefaultClusterFromZKUrl(url)
	}
}

func Retries(backoff *Backoff) option {
	return func(config *RealisConfig) {
		config.backoff = backoff
	}
}

func ThriftJSON() option {
	return func(config *RealisConfig) {
		config.jsonTransport = true
	}
}

func ThriftBinary() option {
	return func(config *RealisConfig) {
		config.binTransport = true
	}
}

func BackOff(b *Backoff) option {
	return func(config *RealisConfig) {
		config.backoff = b
	}
}

func newTJSONTransport(url string, timeout int) (thrift.TTransport, error) {

	trans, err := defaultTTransport(url, timeout)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating realis")
	}
	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "GoRealis v"+VERSION)
	return trans, err
}

func newTBinTransport(url string, timeout int) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout)
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

func NewRealisClient(options ...option) (Realis, error) {
	config := &RealisConfig{}
	fmt.Println(" options length: ", len(options))
	for _, opt := range options {
		opt(config)
	}
	//Default timeout
	if config.timeoutms == 0 {
		config.timeoutms = 10000
	}
	//Set default Transport to JSON if needed.
	if !config.jsonTransport && !config.binTransport {
		config.jsonTransport = true
	}
	var url string
	var err error
	//Cluster or URL?
	if config.cluster != nil {
		url, err = LeaderFromZK(*config.cluster)

		// If ZK is configured, throw an error if the leader is unable to be determined
		if err != nil {
			return nil, errors.Wrap(err, "LeaderFromZK error")
		}

		fmt.Println("schedURLFromZK: ", url)
	} else if config.url != "" {
		fmt.Println("Scheduler URL: ", config.url)
		url = config.url
	} else {
		return nil, errors.New("Incomplete Options -- url or cluster required")
	}

	if config.jsonTransport {
		trans, err := newTJSONTransport(url, config.timeoutms)
		if err != nil {
			return nil, errors.Wrap(err, "Error creating realis")
		}

		config.transport = trans
		config.protoFactory = thrift.NewTJSONProtocolFactory()
	} else if config.binTransport {
		trans, err := newTBinTransport(url, config.timeoutms)
		if err != nil {
			return nil, errors.Wrap(err, "Error creating realis")
		}
		config.transport = trans
		config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}

	//Basic Authentication.
	if config.username != "" && config.password != "" {
		AddBasicAuth(config, config.username, config.password)
	}

	//Set defaultBackoff if required.
	if config.backoff == nil {
		config.backoff = &defaultBackoff
	} else {
		defaultBackoff = *config.backoff
		fmt.Printf(" updating default backoff : %+v\n", *config.backoff)
	}

	fmt.Printf("gorealis config url: %+v\n", config.url)

	return &realisClient{
		config:         config,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(config.transport, config.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(config.transport, config.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(config.transport, config.protoFactory)}, nil

}

// Wrapper object to provide future flexibility
type RealisConfig struct {
	username, password          string
	url                         string
	timeoutms                   int
	binTransport, jsonTransport bool
	cluster                     *Cluster
	backoff                     *Backoff
	transport                   thrift.TTransport
	protoFactory                thrift.TProtocolFactory
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

// Jitter returns a time.Duration between duration and duration + maxFactor *
// duration.
//
// This allows clients to avoid converging on periodic behavior. If maxFactor
// is 0.0, a suggested default value will be chosen.
func Jitter(duration time.Duration, maxFactor float64) time.Duration {
	if maxFactor <= 0.0 {
		maxFactor = 1.0
	}
	wait := duration + time.Duration(rand.Float64()*maxFactor*float64(duration))
	return wait
}

// Create a new Client with Cluster information and other details.

func NewDefaultClientUsingCluster(cluster *Cluster, user, passwd string) (Realis, error) {

	url, err := LeaderFromZK(*cluster)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Printf(" url: %s\n", url)

	//Create new configuration with default transport layer
	config, err := newDefaultConfig(url, 10000)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	config.username = user
	config.password = passwd
	config.cluster = cluster
	config.url = ""
	// Configured for vagrant
	AddBasicAuth(config, user, passwd)
	r := newClient(config)
	return r, nil
}

func GetDefaultClusterFromZKUrl(zkurl string) *Cluster {
	return &Cluster{Name: "defaultCluster",
		AuthMechanism: "UNAUTHENTICATED",
		ZK:            zkurl,
		SchedZKPath:   "/aurora/scheduler",
		AgentRunDir:   "latest",
		AgentRoot:     "/var/lib/mesos",
	}
}

//This api would create default cluster object..
func NewDefaultClientUsingZKUrl(zkUrl, user, passwd string) (Realis, error) {

	fmt.Println(" zkUrl: %s", zkUrl)
	cluster := GetDefaultClusterFromZKUrl(zkUrl)

	url, err := LeaderFromZK(*cluster)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Printf(" url: %s\n", url)

	//Create new configuration with default transport layer
	config, err := newDefaultConfig(url, 10000)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	config.username = user
	config.password = passwd
	config.cluster = cluster
	config.url = ""
	// Configured for vagrant
	AddBasicAuth(config, user, passwd)
	r := newClient(config)
	return r, nil
}

func NewDefaultClientUsingUrl(url, user, passwd string) (Realis, error) {

	fmt.Printf(" url: %s\n", url)
	//Create new configuration with default transport layer
	config, err := newDefaultConfig(url, 10000)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	config.username = user
	config.password = passwd
	config.url = url
	config.cluster = nil
	// Configured for vagrant
	AddBasicAuth(config, user, passwd)
	r := newClient(config)
	return r, nil
}

// Create a new Client with a default transport layer
func newClient(realisconfig *RealisConfig) Realis {
	return &realisClient{
		config:         realisconfig,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(realisconfig.transport, realisconfig.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(realisconfig.transport, realisconfig.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(realisconfig.transport, realisconfig.protoFactory)}
}

// Creates a default Thrift Transport object for communications in gorealis using an HTTP Post Client
func defaultTTransport(urlstr string, timeoutms int) (thrift.TTransport, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return &thrift.THttpClient{}, errors.Wrap(err, "Error creating Cookie Jar")
	}

	trans, err := thrift.NewTHttpPostClientWithOptions(urlstr+"/api",
		thrift.THttpClientOptions{Client: &http.Client{Timeout: time.Millisecond * time.Duration(timeoutms), Jar: jar}})

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
func newDefaultConfig(url string, timeoutms int) (*RealisConfig, error) {
	return newTJSONConfig(url, timeoutms)
}

// Creates a realis config object using HTTP Post and Thrift JSON protocol to communicate with Aurora.
func newTJSONConfig(url string, timeoutms int) (*RealisConfig, error) {
	trans, err := defaultTTransport(url, timeoutms)
	if err != nil {
		return &RealisConfig{}, errors.Wrap(err, "Error creating realis config")
	}

	httpTrans := (trans).(*thrift.THttpClient)
	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "GoRealis v"+VERSION)

	return &RealisConfig{transport: trans, protoFactory: thrift.NewTJSONProtocolFactory()}, nil
}

// Creates a realis config config using HTTP Post and Thrift Binary protocol to communicate with Aurora.
func newTBinaryConfig(url string, timeoutms int) (*RealisConfig, error) {
	trans, err := defaultTTransport(url, timeoutms)
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

func (r *realisClient) ReestablishConn() error {
	//close existing connection..
	fmt.Println("ReestablishConn begin ....")
	r.Close()
	//First check cluster object for re-establish; if not available then try with scheduler url.
	//var config *RealisConfig
	var err error
	var url string

	if r.config.cluster != nil && r.config.username != "" && r.config.password != "" {
		//Re-establish using cluster object.
		url, err = LeaderFromZK(*r.config.cluster)
		if err != nil {
			fmt.Errorf("LeaderFromZK error: %+v\n ", err)
		}
		fmt.Println("ReestablishConn url: ", url)
		if r.config.jsonTransport {
			trans, err := newTJSONTransport(url, r.config.timeoutms)
			if err != nil {
				return errors.Wrap(err, "Error creating realis")
			}
			r.config.transport = trans
			r.config.protoFactory = thrift.NewTJSONProtocolFactory()
		} else if r.config.binTransport {
			trans, err := newTBinTransport(url, r.config.timeoutms)
			if err != nil {
				return errors.Wrap(err, "Error creating realis")
			}
			r.config.transport = trans
			r.config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
		}
		if err != nil {
			fmt.Println("error creating config: ", err)
		}
		// Configured for basic-auth
		AddBasicAuth(r.config, r.config.username, r.config.password)
		r.client = aurora.NewAuroraSchedulerManagerClientFactory(r.config.transport, r.config.protoFactory)
		r.readonlyClient = aurora.NewReadOnlySchedulerClientFactory(r.config.transport, r.config.protoFactory)
		r.adminClient = aurora.NewAuroraAdminClientFactory(r.config.transport, r.config.protoFactory)
	} else if r.config.url != "" && r.config.username != "" && r.config.password != "" {
		//Re-establish using scheduler url.
		fmt.Println("ReestablishConn url: ", r.config.url)
		if r.config.jsonTransport {
			trans, err := newTJSONTransport(url, r.config.timeoutms)
			if err != nil {
				return errors.Wrap(err, "Error creating realis")
			}
			r.config.transport = trans
			r.config.protoFactory = thrift.NewTJSONProtocolFactory()
		} else if r.config.binTransport {
			trans, err := newTBinTransport(url, r.config.timeoutms)
			if err != nil {
				return errors.Wrap(err, "Error creating realis")
			}
			r.config.transport = trans
			r.config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
		}
		AddBasicAuth(r.config, r.config.username, r.config.password)
		r.client = aurora.NewAuroraSchedulerManagerClientFactory(r.config.transport, r.config.protoFactory)
		r.readonlyClient = aurora.NewReadOnlySchedulerClientFactory(r.config.transport, r.config.protoFactory)
		r.adminClient = aurora.NewAuroraAdminClientFactory(r.config.transport, r.config.protoFactory)
	} else {
		fmt.Println(" Missing Data for ReestablishConn ")
		fmt.Println(" r.config.cluster: ", r.config.cluster)
		fmt.Println(" r.config.username: ", r.config.username)
		fmt.Println(" r.config.passwd: ", r.config.password)
		fmt.Println(" r.config.url: ", r.config.url)
		return errors.New(" Missing Data for ReestablishConn ")
	}
	fmt.Printf(" config url before return: %+v\n", r.config.url)
	return nil
}

// Releases resources associated with the realis client.
func (r *realisClient) Close() {
	r.client.Transport.Close()
	r.readonlyClient.Transport.Close()
}

// Uses predefined set of states to retrieve a set of active jobs in Apache Aurora.
func (r *realisClient) GetInstanceIds(key *aurora.JobKey, states map[aurora.ScheduleStatus]bool) (map[int32]bool, error) {
	taskQ := &aurora.TaskQuery{Role: key.Role,
		Environment: key.Environment,
		JobName:     key.Name,
		Statuses:    states}

	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.GetTasksWithoutConfigs(taskQ); err == nil {
			break
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "Error querying Aurora Scheduler for active IDs")
	}
	resp, err = response.ResponseCodeCheck(resp)
	if err != nil {
		return nil, err
	}
	tasks := response.ScheduleStatusResult(resp).GetTasks()
	jobInstanceIds := make(map[int32]bool)
	for _, task := range tasks {
		jobInstanceIds[task.GetAssignedTask().GetInstanceId()] = true
	}
	return jobInstanceIds, nil

}

func (r *realisClient) GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.Response, error) {
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.readonlyClient.GetJobUpdateSummaries(jobUpdateQuery); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}

	}
	return nil, errors.Wrap(err, "Error getting job update summaries from Aurora Scheduler")
}

// Kill specific instances of a job.
func (r *realisClient) KillInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error) {

	instanceIds := make(map[int32]bool)
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration

	for _, instId := range instances {
		instanceIds[instId] = true
	}

	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.KillTasks(key, instanceIds); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}

	}
	return nil, errors.Wrap(err, "Error sending Kill command to Aurora Scheduler")
}

func (r *realisClient) RealisConfig() *RealisConfig {
	return r.config
}

// Sends a kill message to the scheduler for all active tasks under a job.
func (r *realisClient) KillJob(key *aurora.JobKey) (*aurora.Response, error) {

	var instanceIds map[int32]bool
	var err error
	var resp *aurora.Response
	instanceIds, err = r.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err != nil {
		return nil, errors.Wrap(err, "Could not retrieve relevant task instance IDs")
	}

	if len(instanceIds) > 0 {
		defaultBackoff := r.config.backoff
		duration := defaultBackoff.Duration
		for i := 0; i < defaultBackoff.Steps; i++ {
			if i != 0 {
				adjusted := duration
				if defaultBackoff.Jitter > 0.0 {
					adjusted = Jitter(duration, defaultBackoff.Jitter)
				}
				fmt.Println(" sleeping for: ", adjusted)
				time.Sleep(adjusted)
				duration = time.Duration(float64(duration) * defaultBackoff.Factor)
			}

			if resp, err = r.client.KillTasks(key, instanceIds); err == nil {
				return response.ResponseCodeCheck(resp)
			}

			err1 := r.ReestablishConn()
			if err1 != nil {
				fmt.Println("error in ReestablishConn: ", err1)
			}
		}
		if err != nil {
			return nil, errors.Wrap(err, "Error sending Kill command to Aurora Scheduler")
		}
	}
	return nil, errors.New("No tasks in the Active state")
}

// Sends a create job message to the scheduler with a specific job configuration.
func (r *realisClient) CreateJob(auroraJob Job) (*aurora.Response, error) {
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			fmt.Println(" STEPS: ", i)
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		fmt.Println(" calling  CreateJob")
		if resp, err = r.client.CreateJob(auroraJob.JobConfig()); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		fmt.Printf("CreateJob err: %+v\n", err)
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}
	return nil, errors.Wrap(err, "Error sending Create command to Aurora Scheduler")
}

func (r *realisClient) ScheduleCronJob(auroraJob Job) (*aurora.Response, error) {
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}

		if resp, err = r.client.ScheduleCronJob(auroraJob.JobConfig()); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}
	return nil, errors.Wrap(err, "Error sending Cron Job Schedule message to Aurora Scheduler")
}

func (r *realisClient) DescheduleCronJob(key *aurora.JobKey) (*aurora.Response, error) {

	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}

		if resp, err = r.client.DescheduleCronJob(key); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	return nil, errors.Wrap(err, "Error sending Cron Job De-schedule message to Aurora Scheduler")
}

func (r *realisClient) StartCronJob(key *aurora.JobKey) (*aurora.Response, error) {
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}

		if resp, err = r.client.StartCronJob(key); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	return nil, errors.Wrap(err, "Error sending Start Cron Job  message to Aurora Scheduler")
}

// Restarts specific instances specified
func (r *realisClient) RestartInstances(key *aurora.JobKey, instances ...int32) (*aurora.Response, error) {
	instanceIds := make(map[int32]bool)

	for _, instId := range instances {
		instanceIds[instId] = true
	}
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}

		if resp, err = r.client.RestartShards(key, instanceIds); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}

	}
	return nil, errors.Wrap(err, "Error sending Restart command to Aurora Scheduler")
}

// Restarts all active tasks under a job configuration.
func (r *realisClient) RestartJob(key *aurora.JobKey) (*aurora.Response, error) {

	instanceIds, err1 := r.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err1 != nil {
		return nil, errors.Wrap(err1, "Could not retrieve relevant task instance IDs")
	}

	var resp *aurora.Response
	var err error
	if len(instanceIds) > 0 {
		defaultBackoff := r.config.backoff
		duration := defaultBackoff.Duration
		for i := 0; i < defaultBackoff.Steps; i++ {
			if i != 0 {
				adjusted := duration
				if defaultBackoff.Jitter > 0.0 {
					adjusted = Jitter(duration, defaultBackoff.Jitter)
				}
				fmt.Println(" sleeping for: ", adjusted)
				time.Sleep(adjusted)
				duration = time.Duration(float64(duration) * defaultBackoff.Factor)
			}

			if resp, err = r.client.RestartShards(key, instanceIds); err == nil {
				return response.ResponseCodeCheck(resp)
			}
			err1 := r.ReestablishConn()
			if err1 != nil {
				fmt.Println("error in ReestablishConn: ", err1)
			}

		}
		return nil, errors.Wrap(err, "Error sending Restart command to Aurora Scheduler")

	} else {
		return nil, errors.New("No tasks in the Active state")
	}
}

// Update all tasks under a job configuration. Currently gorealis doesn't support for canary deployments.
func (r *realisClient) StartJobUpdate(updateJob *UpdateJob, message string) (*aurora.Response, error) {

	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.StartJobUpdate(updateJob.req, message); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}
	return nil, errors.Wrap(err, "Error sending StartJobUpdate command to Aurora Scheduler")
}

// Abort Job Update on Aurora. Requires the updateId which can be obtained on the Aurora web UI.
func (r *realisClient) AbortJobUpdate(
	updateKey aurora.JobUpdateKey,
	message string) (*aurora.Response, error) {

	var resp *aurora.Response
	var err error
	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.AbortJobUpdate(&updateKey, message); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	return nil, errors.Wrap(err, "Error sending AbortJobUpdate command to Aurora Scheduler")
}

// Scale up the number of instances under a job configuration using the configuration for specific
// instance to scale up.
func (r *realisClient) AddInstances(instKey aurora.InstanceKey, count int32) (*aurora.Response, error) {

	var resp *aurora.Response
	var err error
	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.AddInstances(&instKey, count); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}
	return nil, errors.Wrap(err, "Error sending AddInstances command to Aurora Scheduler")
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
	var err error
	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.GetTasksStatus(query); err == nil {
			break
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "Error querying Aurora Scheduler for task status")
	}
	//Check for response code..
	if resp.GetResponseCode() != aurora.ResponseCode_OK {
		return nil, errors.New(resp.ResponseCode.String() + "--" + response.CombineMessage(resp))
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

// Get information about task including without a task configuration object
func (r *realisClient) GetTasksWithoutConfigs(query *aurora.TaskQuery) (tasks []*aurora.ScheduledTask, e error) {
	var resp *aurora.Response
	var err error
	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.GetTasksWithoutConfigs(query); err == nil {
			break
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "Error querying Aurora Scheduler for task status without configs")
	}
	//Check for response code..
	if resp.GetResponseCode() != aurora.ResponseCode_OK {
		return nil, errors.New(resp.ResponseCode.String() + "--" + response.CombineMessage(resp))
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

func (r *realisClient) FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error) {

	ids := make(map[int32]bool)

	ids[instKey.InstanceId] = true
	taskQ := &aurora.TaskQuery{Role: instKey.JobKey.Role,
		Environment: instKey.JobKey.Environment,
		JobName:     instKey.JobKey.Name,
		InstanceIds: ids,
		Statuses:    aurora.ACTIVE_STATES}

	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}

		if resp, err = r.client.GetTasksStatus(taskQ); err == nil {
			break
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "Error querying Aurora Scheduler for task configuration")
	}

	//Check for response code..
	if resp.GetResponseCode() != aurora.ResponseCode_OK {
		return nil, errors.New(resp.ResponseCode.String() + "--" + response.CombineMessage(resp))
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
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.GetJobUpdateDetails(&updateQuery); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}
	return nil, errors.Wrap(err, "Unable to get job update details")
}

func (r *realisClient) RollbackJobUpdate(key aurora.JobUpdateKey, message string) (*aurora.Response, error) {
	var resp *aurora.Response
	var err error

	defaultBackoff := r.config.backoff
	duration := defaultBackoff.Duration
	for i := 0; i < defaultBackoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if defaultBackoff.Jitter > 0.0 {
				adjusted = Jitter(duration, defaultBackoff.Jitter)
			}
			fmt.Println(" sleeping for: ", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * defaultBackoff.Factor)
		}
		if resp, err = r.client.RollbackJobUpdate(&key, message); err == nil {
			return response.ResponseCodeCheck(resp)
		}
		err1 := r.ReestablishConn()
		if err1 != nil {
			fmt.Println("error in ReestablishConn: ", err1)
		}
	}

	return nil, errors.Wrap(err, "Unable to roll back job update")
}

// Set a list of nodes to DRAINING. This means nothing will be able to be scheduled on them and any existing
// tasks will be killed and re-scheduled elsewhere in the cluster. Tasks from DRAINING nodes are not guaranteed
// to return to running unless there is enough capacity in the cluster to run them.
func (r *realisClient) DrainHosts(hosts ...string) (*aurora.Response, *aurora.DrainHostsResult_, error) {

	var resp *aurora.Response
	var result *aurora.DrainHostsResult_
	var returnErr, clientErr, payloadErr error

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to drain")
	}

	drainList := aurora.NewHosts()
	drainList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		drainList.HostNames[host] = true
	}

	retryErr := ExponentialBackoff(defaultBackoff, func() (bool, error) {

		// Send thrift call, if we have a thrift send error, attempt to reconnect
		// and continue trying to resend command
		if resp, clientErr = r.adminClient.DrainHosts(drainList); clientErr != nil {
			// Experienced an connection error
			err1 := r.ReestablishConn()
			if err1 != nil {
				fmt.Println("error in re-establishing connection: ", err1)
			}
			return false, nil
		}

		// If error is NOT due to connection
		if _, payloadErr = response.ResponseCodeCheck(resp); payloadErr != nil {
			// TODO(rdelvalle): an leader election may cause the response to have
			// failed when it should have succeeded. Retry everything for now until
			// we figure out a more concrete fix.
			return false, nil
		}

		// Successful call
		return true, nil

	})

	if resp != nil && resp.GetResult_() != nil {
		result = resp.GetResult_().GetDrainHostsResult_()
	}

	// Prioritize returning a bad payload error over a client error as a bad payload error indicates
	// a deeper issue
	if payloadErr != nil {
		returnErr = payloadErr
	} else {
		returnErr = clientErr
	}

	// Timed out on retries. *Note that when we fix the unexpected errors with a correct payload,
	// this will can become either a timeout error or a payload error
	if retryErr != nil {
		return resp, result, errors.Wrap(returnErr, "Unable to recover connection")
	}

	return resp, result, nil
}

func (r *realisClient) EndMaintenance(hosts ...string) (*aurora.Response, *aurora.EndMaintenanceResult_, error) {

	var resp *aurora.Response
	var result *aurora.EndMaintenanceResult_
	var returnErr, clientErr, payloadErr error

	if len(hosts) == 0 {
		return nil, nil, errors.New("no hosts provided to drain")
	}

	hostList := aurora.NewHosts()
	hostList.HostNames = make(map[string]bool)
	for _, host := range hosts {
		hostList.HostNames[host] = true
	}

	retryErr := ExponentialBackoff(defaultBackoff, func() (bool, error) {

		// Send thrift call, if we have a thrift send error, attempt to reconnect
		// and continue trying to resend command
		if resp, clientErr = r.adminClient.EndMaintenance(hostList); clientErr != nil {
			// Experienced an connection error
			err1 := r.ReestablishConn()
			if err1 != nil {
				fmt.Println("error in re-establishing connection: ", err1)
			}
			return false, nil
		}

		// If error is NOT due to connection
		if _, payloadErr = response.ResponseCodeCheck(resp); payloadErr != nil {
			// TODO(rdelvalle): an leader election may cause the response to have
			// failed when it should have succeeded. Retry everything for now until
			// we figure out a more concrete fix.
			return false, nil
		}

		// Successful call
		return true, nil

	})

	if resp != nil && resp.GetResult_() != nil {
		result = resp.GetResult_().GetEndMaintenanceResult_()
	}

	// Prioritize returning a bad payload error over a client error as a bad payload error indicates
	// a deeper issue
	if payloadErr != nil {
		returnErr = payloadErr
	} else {
		returnErr = clientErr
	}

	// Timed out on retries. *Note that when we fix the unexpected errors with a correct payload,
	// this will can become either a timeout error or a payload error
	if retryErr != nil {
		return resp, result, errors.Wrap(returnErr, "Unable to recover connection")
	}

	return resp, result, nil
}
