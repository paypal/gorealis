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
	"time"

	"math/rand"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
)

type Backoff struct {
	Duration time.Duration // the base duration
	Factor   float64       // Duration is multipled by factor each iteration
	Jitter   float64       // The amount of jitter applied each iteration
	Steps    int           // Exit with error after this many steps
}

// Jitter returns a time.Duration between duration and duration + maxFactor *
// duration.
//
// This allows clients to avoid converging on periodic behavior. If maxFactor
// is 0.0, a suggested default value will be chosen.
func Jitter(duration time.Duration, maxFactor float64) time.Duration {
	if maxFactor <= 0.0 {
		maxFactor = 1.0
	}
	wait := duration + time.Duration(rand.Float64()*maxFactor*float64(duration))
	return wait
}

// ConditionFunc returns true if the condition is satisfied, or an error
// if the loop should be aborted.
type ConditionFunc func() (done bool, err error)

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
	var err error
	var ok bool
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

		ok, err = condition()

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

	// Provide more information to the user wherever possible
	if err != nil {
		return NewTimeoutError(errors.Wrap(err, "Timed out while retrying"))
	} else {
		return NewTimeoutError(errors.New("Timed out while retrying"))
	}
}

type auroraThriftCall func() (resp *aurora.Response, err error)

// Duplicates the functionality of ExponentialBackoff but is specifically targeted towards ThriftCalls.
func (r *realisClient) ThriftCallWithRetries(thriftCall auroraThriftCall) (*aurora.Response, error) {
	var resp *aurora.Response
	var clientErr error

	backoff := r.config.backoff
	duration := backoff.Duration

	for i := 0; i < backoff.Steps; i++ {

		// If this isn't our first try, backoff before the next try.
		if i != 0 {
			adjusted := duration
			if backoff.Jitter > 0.0 {
				adjusted = Jitter(duration, backoff.Jitter)
			}

			r.logger.Printf("An error occurred during thrift call, backing off for %v before retrying\n", adjusted)

			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * backoff.Factor)
		}

		// Only allow one go-routine make use or modify the thrift client connection.
		r.lock.Lock()
		resp, clientErr = thriftCall()
		r.lock.Unlock()

		// Check if our thrift call is returning an error. This is a retriable event as we don't know
		// if it was caused by network issues.
		if clientErr != nil {
			r.ReestablishConn()

			// In the future, reestablish connection should be able to check if it is actually possible
			// to make a thrift call to Aurora. For now, a reconnect should always lead to a retry.
			continue
		}

		// If there was no client error, but the response is nil, something went wrong.
		// Ideally, we'll never encounter this but we're placing a safeguard here.
		if resp == nil {
			return nil, errors.New("Response from aurora is nil")
		}

		// Check Response Code from thrift and make a decision to continue retrying or not.
		switch responseCode := resp.GetResponseCode(); responseCode {

		// If the thrift call succeeded, stop retrying
		case aurora.ResponseCode_OK:
			return resp, nil

		// If the response code is transient, continue retrying
		case aurora.ResponseCode_ERROR_TRANSIENT:
			r.logger.Println("Aurora replied with Transient error code, retrying")
			continue

		// Failure scenarios, these indicate a bad payload or a bad config. Stop retrying.
		case aurora.ResponseCode_INVALID_REQUEST:
		case aurora.ResponseCode_ERROR:
		case aurora.ResponseCode_AUTH_FAILED:
		case aurora.ResponseCode_JOB_UPDATING_ERROR:
			return nil, errors.New(response.CombineMessage(resp))

		// The only case that should fall down to here is a WARNING response code.
		// It is currently not used as a response in the scheduler so it is unknown how to handle it.
		default:
			return nil, errors.Errorf("unhandled response code from Aurora %v", responseCode.String())
		}

	}

	// Provide more information to the user wherever possible.
	if clientErr != nil {
		return nil, NewTimeoutError(errors.Wrap(clientErr, "Timed out while retrying, including latest error"))
	} else {
		return nil, NewTimeoutError(errors.New("Timed out while retrying"))
	}
}
