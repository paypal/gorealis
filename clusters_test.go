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

package realis_test

import (
	"fmt"
	"testing"

	"github.com/paypal/gorealis"
	"github.com/stretchr/testify/assert"
)

func TestLoadClusters(t *testing.T) {

	clusters, err := realis.LoadClusters("examples/clusters.json")
	if err != nil {
		fmt.Print(err)
	}

	assert.Equal(t, clusters["devcluster"].Name, "devcluster")
	assert.Equal(t, clusters["devcluster"].ZK, "192.168.33.7")
	assert.Equal(t, clusters["devcluster"].SchedZKPath, "/aurora/scheduler")
	assert.Equal(t, clusters["devcluster"].AuthMechanism, "UNAUTHENTICATED")
	assert.Equal(t, clusters["devcluster"].AgentRunDir, "latest")
	assert.Equal(t, clusters["devcluster"].AgentRoot, "/var/lib/mesos")
}
