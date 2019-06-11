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
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

type endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type serviceInstance struct {
	Service             endpoint            `json:"serviceEndpoint"`
	AdditionalEndpoints map[string]endpoint `json:"additionalEndpoints"`
	Status              string              `json:"status"`
}

type zkConfig struct {
	endpoints []string
	path      string
	backoff   Backoff
	timeout   time.Duration
	logger    logger
}

// ZKOpt - Configuration option for the Zookeeper client used.
type ZKOpt func(z *zkConfig)

// ZKEndpoints - Endpoints on which a Zookeeper instance is running to be used by the client.
func ZKEndpoints(endpoints ...string) ZKOpt {
	return func(z *zkConfig) {
		z.endpoints = endpoints
	}
}

// ZKPath - Path to look for information in when connected to Zookeeper.
func ZKPath(path string) ZKOpt {
	return func(z *zkConfig) {
		z.path = path
	}
}

// ZKBackoff - Configuration for Retry mechanism used when connecting to Zookeeper.
// TODO(rdelvalle): Determine if this is really necessary as the ZK library already has a retry built in.
func ZKBackoff(b Backoff) ZKOpt {
	return func(z *zkConfig) {
		z.backoff = b
	}
}

// ZKTimeout - How long to wait on a response from the Zookeeper instance before considering it dead.
func ZKTimeout(d time.Duration) ZKOpt {
	return func(z *zkConfig) {
		z.timeout = d
	}
}

// ZKLogger - Attach a logger to the Zookeeper client in order to debug issues.
func ZKLogger(l logger) ZKOpt {
	return func(z *zkConfig) {
		z.logger = l
	}
}

// LeaderFromZK - Retrieves current Aurora leader from ZK.
func LeaderFromZK(cluster Cluster) (string, error) {
	return LeaderFromZKOpts(ZKEndpoints(strings.Split(cluster.ZK, ",")...), ZKPath(cluster.SchedZKPath))
}

// LeaderFromZKOpts - Retrieves current Aurora leader from ZK with a custom configuration.
func LeaderFromZKOpts(options ...ZKOpt) (string, error) {
	var leaderURL string

	// Load the default configuration for Zookeeper followed by overriding values with those provided by the caller.
	config := &zkConfig{backoff: defaultBackoff, timeout: time.Second * 10, logger: NoopLogger{}}
	for _, opt := range options {
		opt(config)
	}

	if len(config.endpoints) == 0 {
		return "", errors.New("no Zookeeper endpoints supplied")
	}

	if config.path == "" {
		return "", errors.New("no Zookeeper path supplied")
	}

	// Create a closure that allows us to use the ExponentialBackoff function.
	retryErr := ExponentialBackoff(config.backoff, config.logger, func() (bool, error) {

		c, _, err := zk.Connect(config.endpoints, config.timeout, func(c *zk.Conn) { c.SetLogger(config.logger) })
		if err != nil {
			return false, NewTemporaryError(errors.Wrap(err, "failed to connect to Zookeeper"))
		}

		defer c.Close()

		// Open up descriptor for the ZK path given
		children, _, _, err := c.ChildrenW(config.path)
		if err != nil {

			// Sentinel error check as there is no other way to check.
			if err == zk.ErrInvalidPath {
				return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", config.path)
			}

			return false, NewTemporaryError(errors.Wrapf(err, "path %s doesn't exist on Zookeeper ", config.path))
		}

		// Search for the leader through all the children in the given path
		for _, child := range children {

			// Only the leader will start with member_
			if strings.HasPrefix(child, "member_") {

				childPath := config.path + "/" + child
				data, _, err := c.Get(childPath)
				if err != nil {
					if err == zk.ErrInvalidPath {
						return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", childPath)
					}

					return false, NewTemporaryError(errors.Wrap(err, "unable to fetch contents of leader"))
				}

				var serviceInst serviceInstance
				err = json.Unmarshal([]byte(data), &serviceInst)
				if err != nil {
					return false, NewTemporaryError(errors.Wrap(err, "unable to unmarshal contents of leader"))
				}

				// Should only be one endpoint.
				// This should never be encountered as it would indicate Aurora
				// writing bad info into Zookeeper but is kept here as a safety net.
				if len(serviceInst.AdditionalEndpoints) > 1 {
					return false,
						NewTemporaryError(errors.New("ambiguous endpoints in json blob, Aurora wrote bad info to ZK"))
				}

				var scheme, host, port string
				for k, v := range serviceInst.AdditionalEndpoints {
					scheme = k
					host = v.Host
					port = strconv.Itoa(v.Port)
				}

				leaderURL = scheme + "://" + host + ":" + port
				return true, nil
			}
		}

		// Leader data might not be available yet, try to fetch again.
		return false, NewTemporaryError(errors.New("no leader found"))
	})

	if retryErr != nil {
		config.logger.Printf("failed to determine leader after %v attempts", config.backoff.Steps)
		return "", retryErr
	}

	return leaderURL, nil
}
