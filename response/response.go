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

// Helper functions to process aurora.Response
package response

import (
	"github.com/rdelval/gorealis/gen-go/apache/aurora"
)

// Get key from a response created by a StartJobUpdate call
func JobUpdateKey(resp *aurora.Response) *aurora.JobUpdateKey {
	return resp.Result_.StartJobUpdateResult_.GetKey()
}

func JobUpdateDetails(resp *aurora.Response) []*aurora.JobUpdateDetails {
	return resp.Result_.GetJobUpdateDetailsResult_.DetailsList
}
