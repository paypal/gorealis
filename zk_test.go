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
	"log"
	"os"
	"testing"
	"time"

	realis "github.com/paypal/gorealis"
	"github.com/stretchr/testify/assert"
)

var backoff realis.Backoff = realis.Backoff{ // Reduce penalties for this test to make it quick
	Steps:    5,
	Duration: 1 * time.Second,
	Factor:   1.0,
	Jitter:   0.1}

// Test for behavior when no endpoints are given to the ZK leader finding function.
func TestZKNoEndpoints(t *testing.T) {
	_, err := realis.LeaderFromZKOpts()

	assert.Error(t, err)
}

// Test for behavior when no path is given to the ZK leader finding function.
func TestZKNoPath(t *testing.T) {
	_, err := realis.LeaderFromZKOpts(realis.ZKEndpoints("127.0.0.1:2181"))

	assert.Error(t, err)
}

// Test for behavior when a valid but non-existent path is given to the ZK leader finding function.
func TestZKPathDoesntExist(t *testing.T) {
	_, err := realis.LeaderFromZKOpts(realis.ZKEndpoints("127.0.0.1:2181"),
		realis.ZKPath("/somepath"),
		realis.ZKBackoff(backoff),
		realis.ZKLogger(log.New(os.Stdout, "realis-debug: ", log.Ldate)))

	assert.True(t, realis.IsTimeout(err), "a non-existent path should result in a timeout behaving error")

	retryErr := realis.ToRetryCount(err)
	assert.NotNil(t, retryErr, "conversion to retry error failed")
	assert.Equal(t, backoff.Steps, retryErr.RetryCount(), "retry count is off")
}

// Test for behavior when an invalid Zookeeper path is passed. Should fail right away.
func TestZKBadPath(t *testing.T) {
	_, err := realis.LeaderFromZKOpts(realis.ZKEndpoints("127.0.0.1:2181"),
		realis.ZKPath("invalidpath"),
		realis.ZKBackoff(backoff),
		realis.ZKLogger(log.New(os.Stdout, "realis-debug: ", log.Ldate)))

	assert.False(t, realis.IsTimeout(err), "a bad path should result in a NON-timeout behaving error")
}
