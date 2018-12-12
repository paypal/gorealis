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

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

type ResourceType int

const (
	CPU ResourceType = iota
	RAM
	DISK
)

const (
	dedicated  = "dedicated"
	portPrefix = "org.apache.aurora.port."
)

type AuroraTask struct {
	task      *aurora.TaskConfig
	resources map[ResourceType]*aurora.Resource
	portCount int
}

func NewTask() *AuroraTask {
	numCpus := &aurora.Resource{}
	ramMb := &aurora.Resource{}
	diskMb := &aurora.Resource{}

	numCpus.NumCpus = new(float64)
	ramMb.RamMb = new(int64)
	diskMb.DiskMb = new(int64)

	resources := make(map[ResourceType]*aurora.Resource)
	resources[CPU] = numCpus
	resources[RAM] = ramMb
	resources[DISK] = diskMb

	return &AuroraTask{task: &aurora.TaskConfig{
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

// Helper method to convert aurora.TaskConfig to gorealis AuroraTask type
func TaskFromThrift(config *aurora.TaskConfig) *AuroraTask {

	newTask := NewTask()

	// Pass values using receivers as much as possible
	newTask.
		Environment(config.Job.Environment).
		Role(config.Job.Role).
		Name(config.Job.Name).
		MaxFailure(config.MaxTaskFailures).
		IsService(config.IsService)

	if config.Tier != nil {
		newTask.Tier(*config.Tier)
	}

	if config.ExecutorConfig != nil {
		newTask.
			ExecutorName(config.ExecutorConfig.Name).
			ExecutorData(config.ExecutorConfig.Data)
	}

	// Make a deep copy of the task's container
	if config.Container != nil {
		if config.Container.Mesos != nil {
			mesosContainer := NewMesosContainer()

			if config.Container.Mesos.Image != nil {
				if config.Container.Mesos.Image.Appc != nil {
					mesosContainer.AppcImage(config.Container.Mesos.Image.Appc.Name, config.Container.Mesos.Image.Appc.ImageId)
				} else if config.Container.Mesos.Image.Docker != nil {
					mesosContainer.DockerImage(config.Container.Mesos.Image.Docker.Name, config.Container.Mesos.Image.Docker.Tag)
				}
			}

			for _, vol := range config.Container.Mesos.Volumes {
				mesosContainer.AddVolume(vol.ContainerPath, vol.HostPath, vol.Mode)
			}

			newTask.Container(mesosContainer)
		} else if config.Container.Docker != nil {
			dockerContainer := NewDockerContainer()
			dockerContainer.Image(config.Container.Docker.Image)

			for _, param := range config.Container.Docker.Parameters {
				dockerContainer.AddParameter(param.Name, param.Value)
			}

			newTask.Container(dockerContainer)
		}
	}

	// Copy all ports
	for _, resource := range config.Resources {
		// Copy only ports, skip CPU, RAM, and DISK
		if resource != nil {
			if resource.NamedPort != nil {
				newTask.task.Resources = append(newTask.task.Resources, &aurora.Resource{NamedPort: thrift.StringPtr(*resource.NamedPort)})
				newTask.portCount++
			}

			if resource.RamMb != nil {
				newTask.RAM(*resource.RamMb)
			}

			if resource.NumCpus != nil {
				newTask.CPU(*resource.NumCpus)
			}

			if resource.DiskMb != nil {
				newTask.Disk(*resource.DiskMb)
			}
		}
	}

	// Copy constraints
	for _, constraint := range config.Constraints {
		if constraint != nil && constraint.Constraint != nil {

			newConstraint := aurora.Constraint{Name: constraint.Name}

			taskConstraint := constraint.Constraint
			if taskConstraint.Limit != nil {
				newConstraint.Constraint = &aurora.TaskConstraint{Limit: &aurora.LimitConstraint{Limit: taskConstraint.Limit.Limit}}
				newTask.task.Constraints = append(newTask.task.Constraints, &newConstraint)

			} else if taskConstraint.Value != nil {

				values := make([]string, 0)
				for _, val := range taskConstraint.Value.Values {
					values = append(values, val)
				}

				newConstraint.Constraint = &aurora.TaskConstraint{
					Value: &aurora.ValueConstraint{Negated: taskConstraint.Value.Negated, Values: values}}

				newTask.task.Constraints = append(newTask.task.Constraints, &newConstraint)
			}
		}
	}

	// Copy labels
	for _, label := range config.Metadata {
		newTask.task.Metadata = append(newTask.task.Metadata, &aurora.Metadata{Key: label.Key, Value: label.Value})
	}

	// Copy Mesos fetcher URIs
	for _, uri := range config.MesosFetcherUris {
		newTask.task.MesosFetcherUris = append(
			newTask.task.MesosFetcherUris,
			&aurora.MesosFetcherURI{Value: uri.Value, Extract: thrift.BoolPtr(*uri.Extract), Cache: thrift.BoolPtr(*uri.Cache)})
	}

	return newTask
}

// Set AuroraTask Key environment.
func (t *AuroraTask) Environment(env string) *AuroraTask {
	t.task.Job.Environment = env
	return t
}

// Set AuroraTask Key Role.
func (t *AuroraTask) Role(role string) *AuroraTask {
	t.task.Job.Role = role
	return t
}

// Set AuroraTask Key Name.
func (t *AuroraTask) Name(name string) *AuroraTask {
	t.task.Job.Name = name
	return t
}

// Set name of the executor that will the task will be configured to.
func (t *AuroraTask) ExecutorName(name string) *AuroraTask {
	if t.task.ExecutorConfig == nil {
		t.task.ExecutorConfig = aurora.NewExecutorConfig()
	}

	t.task.ExecutorConfig.Name = name
	return t
}

// Will be included as part of entire task inside the scheduler that will be serialized.
func (t *AuroraTask) ExecutorData(data string) *AuroraTask {
	if t.task.ExecutorConfig == nil {
		t.task.ExecutorConfig = aurora.NewExecutorConfig()
	}

	t.task.ExecutorConfig.Data = data
	return t
}

func (t *AuroraTask) CPU(cpus float64) *AuroraTask {
	*t.resources[CPU].NumCpus = cpus
	return t
}

func (t *AuroraTask) RAM(ram int64) *AuroraTask {
	*t.resources[RAM].RamMb = ram
	return t
}

func (t *AuroraTask) Disk(disk int64) *AuroraTask {
	*t.resources[DISK].DiskMb = disk
	return t
}

func (t *AuroraTask) Tier(tier string) *AuroraTask {
	t.task.Tier = &tier
	return t
}

// How many failures to tolerate before giving up.
func (t *AuroraTask) MaxFailure(maxFail int32) *AuroraTask {
	t.task.MaxTaskFailures = maxFail
	return t
}

// Restart the job's tasks if they fail
func (t *AuroraTask) IsService(isService bool) *AuroraTask {
	t.task.IsService = isService
	return t
}

// Add a list of URIs with the same extract and cache configuration. Scheduler must have
// --enable_mesos_fetcher flag enabled. Currently there is no duplicate detection.
func (t *AuroraTask) AddURIs(extract bool, cache bool, values ...string) *AuroraTask {
	for _, value := range values {
		t.task.MesosFetcherUris = append(
			t.task.MesosFetcherUris,
			&aurora.MesosFetcherURI{Value: value, Extract: &extract, Cache: &cache})
	}
	return t
}

// Adds a Mesos label to the job. Note that Aurora will add the
// prefix "org.apache.aurora.metadata." to the beginning of each key.
func (t *AuroraTask) AddLabel(key string, value string) *AuroraTask {
	t.task.Metadata = append(t.task.Metadata, &aurora.Metadata{Key: key, Value: value})
	return t
}

// Add a named port to the job configuration  These are random ports as it's
// not currently possible to request specific ports using Aurora.
func (t *AuroraTask) AddNamedPorts(names ...string) *AuroraTask {
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
func (t *AuroraTask) AddPorts(num int) *AuroraTask {
	start := t.portCount
	t.portCount += num
	for i := start; i < t.portCount; i++ {
		portName := portPrefix + strconv.Itoa(i)
		t.task.Resources = append(t.task.Resources, &aurora.Resource{NamedPort: &portName})
	}

	return t
}

// From Aurora Docs:
// Add a Value constraint
// name - Mesos slave attribute that the constraint is matched against.
// If negated = true , treat this as a 'not' - to avoid specific values.
// Values - list of values we look for in attribute name
func (t *AuroraTask) AddValueConstraint(name string, negated bool, values ...string) *AuroraTask {
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
func (t *AuroraTask) AddLimitConstraint(name string, limit int32) *AuroraTask {
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
func (t *AuroraTask) AddDedicatedConstraint(role, name string) *AuroraTask {
	t.AddValueConstraint(dedicated, false, role+"/"+name)
	return t
}

// Set a container to run for the job configuration to run.
func (t *AuroraTask) Container(container Container) *AuroraTask {
	t.task.Container = container.Build()
	return t
}

func (t *AuroraTask) TaskConfig() *aurora.TaskConfig {
	return t.task
}

func (t *AuroraTask) JobKey() aurora.JobKey {
	return *t.task.Job
}

func (t *AuroraTask) Clone() *AuroraTask {
	newTask := TaskFromThrift(t.task)
	return newTask
}
