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
	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

// Container is an interface that defines a single function needed to create
// an  Aurora container type. It exists because the code must support both Mesos
// and Docker containers.
type Container interface {
	Build() *aurora.Container
}

// MesosContainer is a Mesos style container that can be used by Aurora Jobs.
type MesosContainer struct {
	container *aurora.MesosContainer
}

// DockerContainer is a vanilla Docker style container that can be used by Aurora Jobs.
type DockerContainer struct {
	container *aurora.DockerContainer
}

// NewDockerContainer creates a new  Aurora compatible Docker container configuration.
func NewDockerContainer() DockerContainer {
	return DockerContainer{container: aurora.NewDockerContainer()}
}

// Build creates an Aurora container based upon the configuration provided.
func (c DockerContainer) Build() *aurora.Container {
	return &aurora.Container{Docker: c.container}
}

// Image adds the name of a Docker image to be used by the Job when running.
func (c DockerContainer) Image(image string) DockerContainer {
	c.container.Image = image
	return c
}

// AddParameter adds a parameter to be passed to Docker when the container is run.
func (c DockerContainer) AddParameter(name, value string) DockerContainer {
	c.container.Parameters = append(c.container.Parameters, &aurora.DockerParameter{
		Name:  name,
		Value: value,
	})
	return c
}

// NewMesosContainer creates a Mesos style container to be configured and built for use by an Aurora Job.
func NewMesosContainer() MesosContainer {
	return MesosContainer{container: aurora.NewMesosContainer()}
}

// Build creates a Mesos style Aurora container configuration to be passed on to the Aurora Job.
func (c MesosContainer) Build() *aurora.Container {
	return &aurora.Container{Mesos: c.container}
}

// DockerImage configures the Mesos container to use a specific Docker image when being run.
func (c MesosContainer) DockerImage(name, tag string) MesosContainer {
	if c.container.Image == nil {
		c.container.Image = aurora.NewImage()
	}

	c.container.Image.Docker = &aurora.DockerImage{Name: name, Tag: tag}
	return c
}

// AppcImage configures the Mesos container to use an image in the Appc format to run the container.
func (c MesosContainer) AppcImage(name, imageID string) MesosContainer {
	if c.container.Image == nil {
		c.container.Image = aurora.NewImage()
	}

	c.container.Image.Appc = &aurora.AppcImage{Name: name, ImageId: imageID}
	return c
}
