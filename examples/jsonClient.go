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
	"os"
)

type URIJson struct {
	URI     string `json:"uri"`
	Extract bool   `json:"extract"`
	Cache   bool   `json:"cache"`
}

type JobJson struct {
	Name      string            `json:"name"`
	CPU       float64           `json:"cpu"`
	RAM       int64             `json:"ram_mb"`
	Disk      int64             `json:"disk_mb"`
	Executor  string            `json:"executor"`
	Instances int32             `json:"instances"`
	URIs      []URIJson         `json:"uris"`
	Labels    map[string]string `json:"labels"`
	Service   bool              `json:"service"`
	Ports     int               `json:"ports"`
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

func main() {

	jsonFile := flag.String("file", "", "JSON file containing job definition")
	flag.Parse()

	if *jsonFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	file, err := os.Open(*jsonFile)
	if err != nil {
		fmt.Println("Error opening file ", err)
		os.Exit(1)
	}

	jsonJob := new(JobJson)

	err = json.NewDecoder(file).Decode(jsonJob)
	if err != nil {
		fmt.Println("Error parsing file ", err)
		os.Exit(1)
	}

	jsonJob.Validate()

	//Create new configuration with default transport layer
	config, err := realis.NewDefaultConfig("http://192.168.33.7:8081")
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	realis.AddBasicAuth(&config, "aurora", "secret")
	r := realis.NewClient(config)

	auroraJob := realis.NewJob().
		Environment("prod").
		Role("vagrant").
		Name(jsonJob.Name).
		CPU(jsonJob.CPU).
		RAM(jsonJob.RAM).
		Disk(jsonJob.Disk).
		ExecutorName(jsonJob.Executor).
		InstanceCount(jsonJob.Instances).
		IsService(jsonJob.Service).
		AddPorts(jsonJob.Ports)

	for _, uri := range jsonJob.URIs {
		auroraJob.AddURIs(uri.Extract, uri.Cache, uri.URI)
	}

	for k, v := range jsonJob.Labels {
		auroraJob.AddLabel(k, v)
	}

	resp, err := r.CreateJob(auroraJob)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(resp)
}
