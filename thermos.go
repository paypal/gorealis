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


type ThermosExecutor struct {
    Task ThermosTask `json:"task""`
}

type ThermosTask struct {
    Processes []ThermosProcess `json:"processes"`
    Constraints []ThermosConstraint `json:"constraints"`
    Resources ThermosResources `json:"resources"`
}

type ThermosConstraint struct {
    Order []string `json:"order"`
}

type ThermosResources struct {
    CPU float64 `json:"cpu"`
    Disk int `json:"disk"`
    RAM int `json:"ram"`
    GPU int `json:"gpu"`
}

type ThermosProcess struct {
    Daemon bool `json:"daemon"`
    Name string `json:"name"`
    Ephemeral bool `json:"ephemeral"`
    MaxFailures int `json:"max_failures"`
    MinDuration int `json:"min_duration"`
    Cmdline string `json:"cmdline"`
    Final bool `json:"final`
}


