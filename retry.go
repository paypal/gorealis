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
	"io"
	"math/rand"
	"net/url"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/pkg/errors"
)

// Backoff determines how the retry mechanism should react after each failure and how many failures it should
// tolerate.
type Backoff struct {
	Duration time.Duration // the base duration
	Factor   float64       // Duration is multiplied by a factor each iteration
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

// ExponentialBackoff is a modified version of the Kubernetes exponential-backoff code.
// It repeats a condition check with exponential backoff and checks the condition up to
// Steps times, increasing the wait by multiplying the previous duration by Factor.
//
// If Jitter is greater than zero, a random amount of each duration is added
// (between duration and duration*(1+jitter)).
//
// If the condition never returns true, ErrWaitTimeout is returned. Errors
// do not cause the function to return.
func ExponentialBackoff(backoff Backoff, logger logger, condition ConditionFunc) error {
	var err error
	var ok bool
	var curStep int
	duration := backoff.Duration

	for curStep = 0; curStep < backoff.Steps; curStep++ {

		// Only sleep if it's not the first iteration.
		if curStep != 0 {
			adjusted := duration
			if backoff.Jitter > 0.0 {
				adjusted = Jitter(duration, backoff.Jitter)
			}

			logger.Printf(
				"A retryable error occurred during function call, backing off for %v before retrying\n", adjusted)
			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * backoff.Factor)
		}

		// Execute function passed in.
		ok, err = condition()

		// If the function executed says it succeeded, stop retrying
		if ok {
			return nil
		}

		if err != nil {
			// If the error is temporary, continue retrying.
			if !IsTemporary(err) {
				return err
			}
			// Print out the temporary error we experienced.
			logger.Println(err)
		}
	}

	if curStep > 1 {
		logger.Printf("retried this function call %d time(s)", curStep)
	}

	// Provide more information to the user wherever possible
	if err != nil {
		return newRetryError(errors.Wrap(err, "ran out of retries"), curStep)
	}

	return newRetryError(errors.New("ran out of retries"), curStep)
}

type auroraThriftCall func() (resp *aurora.Response, err error)

// verifyOntimeout defines the type of function that will be used to verify whether a Thirft call to the Scheduler
// made it to the scheduler or not. In general, these types of functions will have to interact with the scheduler
// through the very same Thrift API which previously encountered a time out from the client.
// This means that the functions themselves should be kept to a minimum number of Thrift calls.
// It should also be noted that this is a best effort mechanism and
// is likely to fail for the same reasons that the original call failed.
type verifyOnTimeout func() (*aurora.Response, bool)

// Duplicates the functionality of ExponentialBackoff but is specifically targeted towards ThriftCalls.
func (r *realisClient) thriftCallWithRetries(
	returnOnTimeout bool,
	thriftCall auroraThriftCall,
	verifyOnTimeout verifyOnTimeout) (*aurora.Response, error) {

	var resp *aurora.Response
	var clientErr error
	var curStep int
	timeouts := 0

	backoff := r.config.backoff
	duration := backoff.Duration

	for curStep = 0; curStep < backoff.Steps; curStep++ {

		// If this isn't our first try, backoff before the next try.
		if curStep != 0 {
			adjusted := duration
			if backoff.Jitter > 0.0 {
				adjusted = Jitter(duration, backoff.Jitter)
			}

			r.logger.Printf(
				"A retryable error occurred during thrift call, backing off for %v before retry %v",
				adjusted,
				curStep)

			time.Sleep(adjusted)
			duration = time.Duration(float64(duration) * backoff.Factor)
		}

		// Only allow one go-routine make use or modify the thrift client connection.
		// Placing this in an anonymous function in order to create a new, short-lived stack allowing unlock
		// to be run in case of a panic inside of thriftCall.
		func() {
			r.lock.Lock()
			defer r.lock.Unlock()

			resp, clientErr = thriftCall()

			r.logger.tracePrintf("Aurora Thrift Call ended resp: %v clientErr: %v", resp, clientErr)
		}()

		// Check if our thrift call is returning an error.
		if clientErr != nil {
			// Print out the error to the user
			r.logger.Printf("Client Error: %v", clientErr)

			temporary, timedout := isConnectionError(clientErr)
			if !temporary && r.RealisConfig().failOnPermanentErrors {
				return nil, errors.Wrap(clientErr, "permanent connection error")
			}

			// There exists a corner case where thrift payload was received by Aurora but
			// connection timed out before Aurora was able to reply.
			// Users can take special action on a timeout by using IsTimedout and reacting accordingly
			// if they have configured the client to return on a timeout.
			if timedout && returnOnTimeout {
				return resp, newTimedoutError(errors.New("client connection closed before server answer"))
			}

			// In the future, reestablish connection should be able to check if it is actually possible
			// to make a thrift call to Aurora. For now, a reconnect should always lead to a retry.
			// Ignoring error due to the fact that an error should be retried regardless
			reestablishErr := r.ReestablishConn()
			if reestablishErr != nil {
				r.logger.debugPrintf("error re-establishing connection ", reestablishErr)
			}

			// If users did not opt for a return on timeout in order to react to a timedout error,
			// attempt to verify that the call made it to the scheduler after the connection was re-established.
			if timedout {
				timeouts++
				r.logger.debugPrintf(
					"Client closed connection %d times before server responded, "+
						"consider increasing connection timeout",
					timeouts)

				// Allow caller to provide a function which checks if the original call was successful before
				// it timed out.
				if verifyOnTimeout != nil {
					if verifyResp, ok := verifyOnTimeout(); ok {
						r.logger.Print("verified that the call went through successfully after a client timeout")
						// Response here might be different than the original as it is no longer constructed
						// by the scheduler but mimicked.
						// This is OK since the scheduler is very unlikely to change responses at this point in its
						// development cycle but we must be careful to not return an incorrectly constructed response.
						return verifyResp, nil
					}
				}
			}

			// Retry the thrift payload
			continue
		}

		// If there was no client error, but the response is nil, something went wrong.
		// Ideally, we'll never encounter this but we're placing a safeguard here.
		if resp == nil {
			return nil, errors.New("response from aurora is nil")
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
		case aurora.ResponseCode_INVALID_REQUEST,
			aurora.ResponseCode_ERROR,
			aurora.ResponseCode_AUTH_FAILED,
			aurora.ResponseCode_JOB_UPDATING_ERROR:
			r.logger.Printf("Terminal Response Code %v from Aurora, won't retry\n", resp.GetResponseCode().String())
			return resp, errors.New(response.CombineMessage(resp))

			// The only case that should fall down to here is a WARNING response code.
			// It is currently not used as a response in the scheduler so it is unknown how to handle it.
		default:
			r.logger.debugPrintf("unhandled response code %v received from Aurora\n", responseCode)
			return nil, errors.Errorf("unhandled response code from Aurora %v", responseCode.String())
		}

	}

	if curStep > 1 {
		r.config.logger.Printf("this thrift call was retried %d time(s)", curStep)
	}

	// Provide more information to the user wherever possible.
	if clientErr != nil {
		return nil, newRetryError(errors.Wrap(clientErr, "ran out of retries, including latest error"), curStep)
	}

	return nil, newRetryError(errors.New("ran out of retries"), curStep)
}

// isConnectionError processes the error received by the client.
// The return values indicate weather this was determined to be a temporary error
// and weather it was determined to be a timeout error
func isConnectionError(err error) (bool, bool) {

	// Determine if error is a temporary URL error by going up the stack
	transportException, ok := err.(thrift.TTransportException)
	if !ok {
		return false, false
	}

	urlError, ok := transportException.Err().(*url.Error)
	if !ok {
		return false, false
	}

	// EOF error occurs when the server closes the read buffer of the client. This is common
	// when the server is overloaded and we consider it temporary.
	// All other which are not temporary as per the member function Temporary(),
	// are considered not temporary (permanent).
	if urlError.Err != io.EOF && !urlError.Temporary() {
		return false, false
	}

	return true, urlError.Timeout()
}
