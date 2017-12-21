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
	"bytes"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/pkg/errors"
)

// Get key from a response created by a StartJobUpdate call
func JobUpdateKey(resp *aurora.Response) *aurora.JobUpdateKey {
	return resp.GetResult_().GetStartJobUpdateResult_().GetKey()
}

func JobUpdateDetails(resp *aurora.Response) []*aurora.JobUpdateDetails {
	return resp.GetResult_().GetGetJobUpdateDetailsResult_().GetDetailsList()
}

func ScheduleStatusResult(resp *aurora.Response) *aurora.ScheduleStatusResult_ {
	return resp.GetResult_().GetScheduleStatusResult_()
}

func JobUpdateSummaries(resp *aurora.Response) []*aurora.JobUpdateSummary {
	return resp.GetResult_().GetGetJobUpdateSummariesResult_().GetUpdateSummaries()
}

func ResponseCodeCheck(resp *aurora.Response) (*aurora.Response, error) {
	if resp == nil {
		return resp, errors.New("Response is nil")
	}
	if resp.GetResponseCode() != aurora.ResponseCode_OK {
		return resp, errors.New(CombineMessage(resp))
	}

	return resp, nil
}

// Based on aurora client: src/main/python/apache/aurora/client/base.py
func CombineMessage(resp *aurora.Response) string {
	var buffer bytes.Buffer
	for _, detail := range resp.GetDetails() {
		buffer.WriteString(detail.GetMessage() + ", ")
	}

	if buffer.Len() > 0 {
		buffer.Truncate(buffer.Len() - 2) // Get rid of trailing comma + space
	}
	return buffer.String()
}
