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

type Container interface {
	Build() *aurora.Container
}

type MesosContainer struct {
	container *aurora.MesosContainer
}

type DockerContainer struct {
	container *aurora.DockerContainer
}

func NewDockerContainer() DockerContainer {
	return DockerContainer{container: aurora.NewDockerContainer()}
}

func (c DockerContainer) Build() *aurora.Container {
	return &aurora.Container{Docker: c.container}
}

func (c DockerContainer) Image(image string) DockerContainer {
	c.container.Image = image
	return c
}

func (c DockerContainer) AddParameter(name, value string) DockerContainer {
	c.container.Parameters = append(c.container.Parameters, &aurora.DockerParameter{
		Name:  name,
		Value: value,
	})
	return c
}

func NewMesosContainer() MesosContainer {
	return MesosContainer{container: aurora.NewMesosContainer()}
}

func (c MesosContainer) Build() *aurora.Container {
	return &aurora.Container{Mesos: c.container}
}

func (c MesosContainer) DockerImage(name, tag string) MesosContainer {
	if c.container.Image == nil {
		c.container.Image = aurora.NewImage()
	}

	c.container.Image.Docker = &aurora.DockerImage{Name: name, Tag: tag}
	return c
}

func (c MesosContainer) AppcImage(name, imageId string) MesosContainer {
	if c.container.Image == nil {
		c.container.Image = aurora.NewImage()
	}

	c.container.Image.Appc = &aurora.AppcImage{Name: name, ImageId: imageId}
	return c
}
