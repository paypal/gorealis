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
	"encoding/json"
	"github.com/pkg/errors"
	"os"
)

type Cluster struct {
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

// Loads clusters.json file traditionally located at /etc/aurora/clusters.json
func LoadClusters(config string) (map[string]Cluster, error) {

	file, err := os.Open(config)
	if err != nil {
		return nil, errors.Wrap(err, "Error opening file "+config)
	}

	clusters := make([]Cluster, 1)
	err = json.NewDecoder(file).Decode(&clusters)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing file "+config)
	}

	m := make(map[string]Cluster)
	for _, cluster := range clusters {
		m[cluster.Name] = cluster
	}

	return m, nil
}
