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

import "encoding/json"

type ThermosExecutor struct {
	Task  ThermosTask        `json:"task""`
	order *ThermosConstraint `json:"-"`
}

type ThermosTask struct {
	Processes   map[string]*ThermosProcess `json:"processes"`
	Constraints []*ThermosConstraint       `json:"constraints"`
	Resources   thermosResources           `json:"resources"`
}

type ThermosConstraint struct {
	Order []string `json:"order,omitempty"`
}

// This struct should always be controlled by the Aurora job struct.
// Therefore it is private.
type thermosResources struct {
	CPU  *float64 `json:"cpu,omitempty"`
	Disk *int64   `json:"disk,omitempty"`
	RAM  *int64   `json:"ram,omitempty"`
	GPU  *int64   `json:"gpu,omitempty"`
}

type ThermosProcess struct {
	Name        string `json:"name"`
	Cmdline     string `json:"cmdline"`
	Daemon      bool   `json:"daemon"`
	Ephemeral   bool   `json:"ephemeral"`
	MaxFailures int    `json:"max_failures"`
	MinDuration int    `json:"min_duration"`
	Final       bool   `json:"final"`
}

func NewThermosProcess(name, command string) ThermosProcess {
	return ThermosProcess{
		Name:        name,
		Cmdline:     command,
		MaxFailures: 1,
		Daemon:      false,
		Ephemeral:   false,
		MinDuration: 5,
		Final:       false}
}

// Processes must have unique names. Adding a process whose name already exists will
// result in overwriting the previous version of the process.
func (t *ThermosExecutor) AddProcess(process ThermosProcess) *ThermosExecutor {
	if len(t.Task.Processes) == 0 {
		t.Task.Processes = make(map[string]*ThermosProcess, 0)
	}

	t.Task.Processes[process.Name] = &process

	// Add Process to order
	t.addToOrder(process.Name)
	return t
}

// Only constraint that should be added for now is the order of execution, therefore this
// receiver is private.
func (t *ThermosExecutor) addConstraint(constraint *ThermosConstraint) *ThermosExecutor {
	if len(t.Task.Constraints) == 0 {
		t.Task.Constraints = make([]*ThermosConstraint, 0)
	}

	t.Task.Constraints = append(t.Task.Constraints, constraint)
	return t
}

// Order in which the Processes should be executed. Index 0 will be executed first, index N will be executed last.
func (t *ThermosExecutor) ProcessOrder(order ...string) *ThermosExecutor {
	if t.order == nil {
		t.order = &ThermosConstraint{}
		t.addConstraint(t.order)
	}

	t.order.Order = order
	return t
}

// Add Process to execution order. By default this is a FIFO setup. Custom order can be given by overriding
// with ProcessOrder
func (t *ThermosExecutor) addToOrder(name string) {
	if t.order == nil {
		t.order = &ThermosConstraint{Order: make([]string, 0)}
		t.addConstraint(t.order)
	}

	t.order.Order = append(t.order.Order, name)
}

// Ram is determined by the job object.
func (t *ThermosExecutor) ram(ram int64) {
	// Convert from bytes to MiB
	ram *= 1024 ^ 2
	t.Task.Resources.RAM = &ram
}

// Disk is determined by the job object.
func (t *ThermosExecutor) disk(disk int64) {
	// Convert from bytes to MiB
	disk *= 1024 ^ 2
	t.Task.Resources.Disk = &disk
}

// CPU is determined by the job object.
func (t *ThermosExecutor) cpu(cpu float64) {
	t.Task.Resources.CPU = &cpu
}

// GPU is determined by the job object.
func (t *ThermosExecutor) gpu(gpu int64) {
	t.Task.Resources.GPU = &gpu
}

// Deep copy of Thermos executor
func (t *ThermosExecutor) Clone() *ThermosExecutor {
	tNew := ThermosExecutor{}

	if t.order != nil {
		tNew.order = &ThermosConstraint{Order: t.order.Order}

		tNew.addConstraint(tNew.order)
	}

	tNew.Task.Processes = make(map[string]*ThermosProcess)

	for name, process := range t.Task.Processes {
		newProcess := *process
		tNew.Task.Processes[name] = &newProcess
	}

	tNew.Task.Resources = t.Task.Resources

	return &tNew
}

type thermosTaskJSON struct {
	Processes   []*ThermosProcess    `json:"processes"`
	Constraints []*ThermosConstraint `json:"constraints"`
	Resources   thermosResources     `json:"resources"`
}

// Custom Marshaling for Thermos Task to match what Thermos expects
func (t *ThermosTask) MarshalJSON() ([]byte, error) {

	// Convert map to array to match what Thermos expects
	processes := make([]*ThermosProcess, 0)
	for _, process := range t.Processes {
		processes = append(processes, process)
	}

	return json.Marshal(&thermosTaskJSON{
		Processes:   processes,
		Constraints: t.Constraints,
		Resources:   t.Resources,
	})
}

// Custom Unmarshaling to match what Thermos would contain
func (t *ThermosTask) UnmarshalJSON(data []byte) error {

	// Thermos format
	aux := &thermosTaskJSON{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	processes := make(map[string]*ThermosProcess)
	for _, process := range aux.Processes {
		processes[process.Name] = process
	}

	return nil
}
