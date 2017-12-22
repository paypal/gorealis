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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type ServiceInstance struct {
	Service             Endpoint            `json:"serviceEndpoint"`
	AdditionalEndpoints map[string]Endpoint `json:"additionalEndpoints"`
	Status              string              `json:"status"`
}

// Retrieves current Aurora leader from ZK.
func LeaderFromZK(cluster Cluster) (string, error) {

	var zkurl string

	retryErr := ExponentialBackoff(defaultBackoff, func() (bool, error) {

		endpoints := strings.Split(cluster.ZK, ",")

		//TODO (rdelvalle): When enabling debugging, change logger here
		c, _, err := zk.Connect(endpoints, time.Second*10, func(c *zk.Conn) { c.SetLogger(NoopLogger{}) })
		if err != nil {
			return false, errors.Wrap(err, "Failed to connect to Zookeeper at "+cluster.ZK)
		}

		defer c.Close()

		// Open up descriptor for the ZK path given
		children, _, _, err := c.ChildrenW(cluster.SchedZKPath)
		if err != nil {
			return false, errors.Wrapf(err, "Path %s doesn't exist on Zookeeper ", cluster.SchedZKPath)
		}

		// Search for the leader through all the children in the given path
		serviceInst := new(ServiceInstance)
		for _, child := range children {

			// Only the leader will start with member_
			if strings.HasPrefix(child, "member_") {

				data, _, err := c.Get(cluster.SchedZKPath + "/" + child)
				if err != nil {
					return false, errors.Wrap(err, "Error fetching contents of leader")
				}

				err = json.Unmarshal([]byte(data), serviceInst)
				if err != nil {
					return false, errors.Wrap(err, "Unable to unmarshall contents of leader")
				}

				// Should only be one endpoint
				if len(serviceInst.AdditionalEndpoints) > 1 {
					fmt.Errorf("Ambiguous end points schemes")
				}

				var scheme, host, port string
				for k, v := range serviceInst.AdditionalEndpoints {
					scheme = k
					host = v.Host
					port = strconv.Itoa(v.Port)
				}

				zkurl = scheme + "://" + host + ":" + port
				return true, nil
			}
		}

		return false, errors.New("No leader found")
	})

	if retryErr != nil {
		return "", errors.Wrapf(retryErr, "Failed to determine leader after %v attempts", defaultBackoff.Steps)
	}

	return zkurl, nil
}
