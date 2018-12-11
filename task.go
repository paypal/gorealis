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
	"strconv"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

type Task struct {
	task      *aurora.TaskConfig
	resources map[string]*aurora.Resource
	portCount int
}

func NewTask() *Task {
	numCpus := &aurora.Resource{}
	ramMb := &aurora.Resource{}
	diskMb := &aurora.Resource{}

	numCpus.NumCpus = new(float64)
	ramMb.RamMb = new(int64)
	diskMb.DiskMb = new(int64)

	resources := make(map[string]*aurora.Resource)
	resources["cpu"] = numCpus
	resources["ram"] = ramMb
	resources["disk"] = diskMb

	return &Task{task: &aurora.TaskConfig{
		Job:              &aurora.JobKey{},
		MesosFetcherUris: make([]*aurora.MesosFetcherURI, 0),
		Metadata:         make([]*aurora.Metadata, 0),
		Constraints:      make([]*aurora.Constraint, 0),
		// Container is a Union so one container field must be set. Set Mesos by default.
		Container: NewMesosContainer().Build(),
		Resources: []*aurora.Resource{numCpus, ramMb, diskMb},
	},
		resources: resources,
		portCount: 0}
}

// Set Task Key environment.
func (t *Task) Environment(env string) *Task {
	t.task.Job.Environment = env
	return t
}

// Set Task Key Role.
func (t *Task) Role(role string) *Task {
	t.task.Job.Role = role
	return t
}

// Set Task Key Name.
func (t *Task) Name(name string) *Task {
	t.task.Job.Name = name
	return t
}

// Set name of the executor that will the task will be configured to.
func (t *Task) ExecutorName(name string) *Task {

	if t.task.ExecutorConfig == nil {
		t.task.ExecutorConfig = aurora.NewExecutorConfig()
	}

	t.task.ExecutorConfig.Name = name
	return t
}

// Will be included as part of entire task inside the scheduler that will be serialized.
func (t *Task) ExecutorData(data string) *Task {

	if t.task.ExecutorConfig == nil {
		t.task.ExecutorConfig = aurora.NewExecutorConfig()
	}

	t.task.ExecutorConfig.Data = data
	return t
}

func (t *Task) CPU(cpus float64) *Task {
	*t.resources["cpu"].NumCpus = cpus

	return t
}

func (t *Task) RAM(ram int64) *Task {
	*t.resources["ram"].RamMb = ram

	return t
}

func (t *Task) Disk(disk int64) *Task {
	*t.resources["disk"].DiskMb = disk

	return t
}

func (t *Task) Tier(tier string) *Task {
	*t.task.Tier = tier

	return t
}

// How many failures to tolerate before giving up.
func (t *Task) MaxFailure(maxFail int32) *Task {
	t.task.MaxTaskFailures = maxFail
	return t
}

// Restart the job's tasks if they fail
func (t *Task) IsService(isService bool) *Task {
	t.task.IsService = isService
	return t
}

// Add a list of URIs with the same extract and cache configuration. Scheduler must have
// --enable_mesos_fetcher flag enabled. Currently there is no duplicate detection.
func (t *Task) AddURIs(extract bool, cache bool, values ...string) *Task {
	for _, value := range values {
		t.task.MesosFetcherUris = append(
			t.task.MesosFetcherUris,
			&aurora.MesosFetcherURI{Value: value, Extract: &extract, Cache: &cache})
	}
	return t
}

// Adds a Mesos label to the job. Note that Aurora will add the
// prefix "org.apache.aurora.metadata." to the beginning of each key.
func (t *Task) AddLabel(key string, value string) *Task {
	t.task.Metadata = append(t.task.Metadata, &aurora.Metadata{Key: key, Value: value})
	return t
}

// Add a named port to the job configuration  These are random ports as it's
// not currently possible to request specific ports using Aurora.
func (t *Task) AddNamedPorts(names ...string) *Task {
	t.portCount += len(names)
	for _, name := range names {
		t.task.Resources = append(t.task.Resources, &aurora.Resource{NamedPort: &name})
	}

	return t
}

// Adds a request for a number of ports to the job configuration. The names chosen for these ports
// will be org.apache.aurora.port.X, where X is the current port count for the job configuration
// starting at 0. These are random ports as it's not currently possible to request
// specific ports using Aurora.
func (t *Task) AddPorts(num int) *Task {
	start := t.portCount
	t.portCount += num
	for i := start; i < t.portCount; i++ {
		portName := "org.apache.aurora.port." + strconv.Itoa(i)
		t.task.Resources = append(t.task.Resources, &aurora.Resource{NamedPort: &portName})
	}

	return t
}

// From Aurora Docs:
// Add a Value constraint
// name - Mesos slave attribute that the constraint is matched against.
// If negated = true , treat this as a 'not' - to avoid specific values.
// Values - list of values we look for in attribute name
func (t *Task) AddValueConstraint(name string, negated bool, values ...string) *Task {
	t.task.Constraints = append(t.task.Constraints,
		&aurora.Constraint{
			Name: name,
			Constraint: &aurora.TaskConstraint{
				Value: &aurora.ValueConstraint{
					Negated: negated,
					Values:  values,
				},
				Limit: nil,
			},
		})

	return t
}

// From Aurora Docs:
// A constraint that specifies the maximum number of active tasks on a host with
// a matching attribute that may be scheduled simultaneously.
func (t *Task) AddLimitConstraint(name string, limit int32) *Task {
	t.task.Constraints = append(t.task.Constraints,
		&aurora.Constraint{
			Name: name,
			Constraint: &aurora.TaskConstraint{
				Value: nil,
				Limit: &aurora.LimitConstraint{Limit: limit},
			},
		})

	return t
}

// From Aurora Docs:
// dedicated attribute. Aurora treats this specially, and only allows matching jobs
// to run on these machines, and will only schedule matching jobs on these machines.
// When a job is created, the scheduler requires that the $role component matches
// the role field in the job configuration, and will reject the job creation otherwise.
// A wildcard (*) may be used for the role portion of the dedicated attribute, which
// will allow any owner to elect for a job to run on the host(s)
func (t *Task) AddDedicatedConstraint(role, name string) *Task {
	t.AddValueConstraint("dedicated", false, role+"/"+name)

	return t
}

// Set a container to run for the job configuration to run.
func (t *Task) Container(container Container) *Task {
	t.task.Container = container.Build()

	return t
}
func (t *Task) TaskConfig() *aurora.TaskConfig {
	return t.task
}
