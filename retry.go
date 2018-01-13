/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package realis

import (
	"errors"
	"time"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

const (
	ConnRefusedErr   = "connection refused"
	NoLeaderFoundErr = "No leader found"
)

var RetryConnErr = errors.New("error occured during with aurora retrying")

// ConditionFunc returns true if the condition is satisfied, or an error
// if the loop should be aborted.
type ConditionFunc func() (done bool, err error)

type AuroraThriftCall func() (resp *aurora.Response, err error)

// Modified version of the Kubernetes exponential-backoff code.
// ExponentialBackoff repeats a condition check with exponential backoff.
//
// It checks the condition up to Steps times, increasing the wait by multiplying
// the previous duration by Factor.
//
// If Jitter is greater than zero, a random amount of each duration is added
// (between duration and duration*(1+jitter)).
//
// If the condition never returns true, ErrWaitTimeout is returned. All other
// errors terminate immediately.
func ExponentialBackoff(backoff Backoff, condition ConditionFunc) error {
	duration := backoff.Duration
	for i := 0; i < backoff.Steps; i++ {
		if i != 0 {
			adjusted := duration
			if backoff.Jitter > 0.0 {
				adjusted = Jitter(duration, backoff.Jitter)
			}
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * backoff.Factor)
		}

		ok, err := condition()

		// If the function executed says it succeeded, stop retrying
		if ok {
			return nil
		}

		// Stop retrying if the error is NOT temporary.
		if err != nil {
			if !IsTemporary(err) {
				return err
			}
		}

	}
	return NewTimeoutError(errors.New("Timed out while retrying"))
}

// CheckAndRetryConn function takes realis client and a trhift API function to call and returns response and error
// If Error from the APi call is Retry able . THe functions re establishes the connection with aurora by getting the latest aurora master from zookeeper.
// If Error is retyable return resp and RetryConnErr error.
func CheckAndRetryConn(r Realis, auroraCall AuroraThriftCall) (*aurora.Response, error) {
	resp, cliErr := auroraCall()

	// TODO: Rerturn different error type based on the error that was returned by the API call
	if cliErr != nil {
		r.ReestablishConn()
		return resp, NewPermamentError(RetryConnErr)
	}
	if resp != nil && resp.GetResponseCode() == aurora.ResponseCode_ERROR_TRANSIENT {
		return resp, NewTemporaryError(errors.New("Aurora scheduler temporarily unavailable"))
	}
	return resp, nil
}
