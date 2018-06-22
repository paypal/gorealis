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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"os"
)

type URIJson struct {
	URI     string `json:"uri"`
	Extract bool   `json:"extract"`
	Cache   bool   `json:"cache"`
}

type JobJson struct {
	Name             string            `json:"name"`
	CPU              float64           `json:"cpu"`
	RAM              int64             `json:"ram_mb"`
	Disk             int64             `json:"disk_mb"`
	Executor         string            `json:"executor"`
	ExecutorDataFile string            `json:"execDataFile,omitempty"`
	Instances        int32             `json:"instances"`
	URIs             []URIJson         `json:"uris"`
	Labels           map[string]string `json:"labels"`
	Service          bool              `json:"service"`
	Ports            int               `json:"ports"`
}

func (j *JobJson) Validate() bool {

	if j.Name == "" {
		return false
	}

	if j.CPU <= 0.0 {
		return false
	}

	if j.RAM <= 0 {
		return false
	}

	if j.Disk <= 0 {
		return false
	}

	return true
}

type ZKClusterConfig struct {
	Name          string `json:"name"`
	AgentRoot     string `json:"slave_root"`
	AgentRunDir   string `json:"slave_run_directory"`
	ZK            string `json:"zk"`
	ZKPort        int    `json:"zk_port"`
	SchedZKPath   string `json:"scheduler_zk_path"`
	SchedURI      string `json:"scheduler_uri"`
	ProxyURL      string `json:"proxy_url"`
	AuthMechanism string `json:"auth_mechanism"`
}

type ConfigJson struct {
	Username      string           `json:"username"`
	Password      string           `json:"password"`
	SchedUrl      string           `json:"schedUrl"`
	BinTransport  bool             `json:"bin_transport,omitempty"`
	JsonTransport bool             `json:"json_transport,omitempty"`
	ClusterConfig *ZKClusterConfig `json:"zkCluster"`
	Debug         bool             `json:"debug,omitempty"`
}

// Command-line arguments for config and job JSON files.
var configJSONFile, jobJSONFile string

var job JobJson
var config ConfigJson

// Reading command line arguments and validating.
// If Aurora scheduler URL not provided, then using zookeeper to locate the leader.
func init() {
	flag.StringVar(&configJSONFile, "config", "./config.json", "The config file that contains username, password, and the cluster configuration information.")
	flag.StringVar(&jobJSONFile, "job", "./job.json", "JSON file containing job definitions.")

	flag.Parse()

	job = new(JobJson)
	config = new(realis.RealisConfig)

	if jobsFile, jobJSONReadErr := os.Open(*jobJSONFile); jobJSONReadErr != nil {
		flag.Usage()
		fmt.Println("Error reading the job JSON file: ", jobJSONReadErr)
		os.Exit(1)
	} else {
		if unmarshallErr := json.NewDecoder(jobsFile).Decode(job); unmarshallErr != nil {
			flag.Usage()
			fmt.Println("Error parsing job json file: ", unmarshallErr)
			os.Exit(1)
		}

		// Need to validate the job JSON file.
		if !job.Validate() {
			fmt.Println("Invalid Job.")
			os.Exit(1)
		}
	}

	if configFile, configJSONErr := os.Open(*configJSONFile); configJSONErr {
		flag.Usage()
		fmt.Println("Error reading the config JSON file: ", configJSONErr)
		os.Exit(1)
	} else {
		if unmarshallErr := json.NewDecoder(configFile).Decode(config); unmarshallErr {
			fmt.Println("Error parsing config JSON file: ", unmarshallErr)
			os.Exit(1)
		}
	}
}

func main() {
	var transportOption realis.ClientOption
	if config.BinTransport {
		transportOption = realis.ThriftBinary()
	} else {
		transportOption = realis.ThriftJSON()
	}

	clientOptions := []realis.ClientOption{
		realis.BasicAuth(config.Username, config.Password),
		transportOption(),
		realis.ZKCluster(config.ClusterConfig),
		realis.SchedulerUrl(config.SchedUrl),
	}

	if config.Debug {
		clientOptions = append(clientOptions, realis.Debug())
	}

	if r, clientCreationErr := realis.NewRealisClient(clientOptions...); clientCreationErr != nil {
		fmt.Println(clientCreationErr)
		os.Exit(1)
	} else {
		monitor := &realis.Monitor{Client: r}
		defer r.Close()
		auroraJob := realis.NewJob().
			Environment("prod").
			Role("vagrant").
			Name(job.Name).
			ExecutorName(job.Executor).
			CPU(job.CPU).
			RAM(job.RAM).
			Disk(job.Disk).
			IsService(job.Service).
			InstanceCount(job.Instances).
			AddPorts(job.Ports)

		// If thermos executor, then reading in the thermos payload.
		if (job.Executor == aurora.AURORA_EXECUTOR_NAME) || (job.Executor == "thermos") {
			payload, err := ioutil.ReadFile(job.ExecutorDataFile)
			if err != nil {
				fmt.Println(errors.Wrap(err, "Invalid thermos payload file!"))
				os.Exit(1)
			}
			auroraJob.ExecutorName(aurora.AURORA_EXECUTOR_NAME).
				ExecutorData(string(payload))
		} else {
			auroraJob.ExecutorName(job.Executor)
		}

		// Adding URIs.
		for _, uri := range uris {
			auroraJob.AddURIs(uri.Extract, uri.Cache, uri.URI)
		}

		// Adding Labels.
		for key, value := range labels {
			auroraJob.AddLabel(key, value)
		}

		fmt.Println("Creating Job...")
		if resp, jobCreationErr := r.CreateJob(auroraJob); jobCreationErr != nil {
			fmt.Println("Error creating Aurora job: ", jobCreationErr)
			os.Exit(1)
		} else {
			if resp.ResponseCode == aurora.ResponseCode_OK {
				if ok, monitorErr := monitor.Instances(auroraJob.JobKey(), auroraJob.GetInstanceCount(), 5, 50); !ok || monitorErr != nil {
					if _, jobErr := r.KillJob(auroraJob.JobKey()); jobErr !=
						nil {
						fmt.Println(jobErr)
						os.Exit(1)
					} else {
						fmt.Println("ok: ", ok)
						fmt.Println("jobErr: ", jobErr)
					}
				}
			}
		}
	}
}
