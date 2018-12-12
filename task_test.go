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
	"testing"

	realis "github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/stretchr/testify/assert"
)

func TestAuroraTask_Clone(t *testing.T) {

	task0 := realis.NewTask().
		Environment("development").
		Role("ubuntu").
		Name("this_is_a_test").
		ExecutorName(aurora.AURORA_EXECUTOR_NAME).
		ExecutorData("{fake:payload}").
		CPU(10).
		RAM(643).
		Disk(1000).
		IsService(true).
		AddPorts(10).
		Tier("preferred").
		MaxFailure(23).
		AddURIs(true, true, "testURI").
		AddLabel("Test", "Value").
		AddNamedPorts("test").
		AddValueConstraint("test", false, "testing").
		AddLimitConstraint("test_limit", 1).
		AddDedicatedConstraint("ubuntu", "name").
		Container(realis.NewDockerContainer().AddParameter("hello", "world").Image("testImg"))

	task1 := task0.Clone()

	assert.EqualValues(t, task0, task1, "Clone does not return the correct deep copy of AuroraTask")

	task0.Container(realis.NewMesosContainer().
		AppcImage("test", "testing").
		AddVolume("test", "test", aurora.Mode_RW))
	task2 := task0.Clone()
	assert.EqualValues(t, task0, task2, "Clone does not return the correct deep copy of AuroraTask")
}
