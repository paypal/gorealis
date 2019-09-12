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
	"strings"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

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
