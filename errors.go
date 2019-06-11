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

// Using a pattern described by Dave Cheney to differentiate errors
// https://dave.cheney.net/2016/04/27/dont-just-check-errors-handle-them-gracefully

// Timeout errors are returned when a function is unable to continue executing due
// to a time constraint or meeting a set number of retries.
type timeout interface {
	Timedout() bool
}

// IsTimeout returns true if the error being passed as an argument implements the Timeout interface
// and the Timedout function returns true.
func IsTimeout(err error) bool {
	temp, ok := err.(timeout)
	return ok && temp.Timedout()
}

type timeoutErr struct {
	error
	timedout bool
}

func (r *timeoutErr) Timedout() bool {
	return r.timedout
}

func newTimedoutError(err error) *timeoutErr {
	return &timeoutErr{error: err, timedout: true}
}

// retryErr is a superset of timeout which includes extra context
// with regards to our retry mechanism. This is done in order to make sure
// that our retry mechanism works as expected through our tests and should
// never be relied on or used directly. It is not made part of the public API
// on purpose.
type retryErr struct {
	error
	timedout   bool
	retryCount int // How many times did the mechanism retry the command
}

// Retry error is a timeout type error with added context.
func (r *retryErr) Timedout() bool {
	return r.timedout
}

func (r *retryErr) RetryCount() int {
	return r.retryCount
}

// ToRetryCount is a helper function for testing verification to avoid whitebox testing
// as well as keeping retryErr as a private.
// Should NOT be used under any other context.
func ToRetryCount(err error) *retryErr {
	if retryErr, ok := err.(*retryErr); ok {
		return retryErr
	}
	return nil
}

func newRetryError(err error, retryCount int) *retryErr {
	return &retryErr{error: err, timedout: true, retryCount: retryCount}
}

// Temporary errors indicate that the action may or should be retried.
type temporary interface {
	Temporary() bool
}

// IsTemporary indicates whether the error passed in as an argument implements the temporary interface
// and if the Temporary function returns true.
func IsTemporary(err error) bool {
	temp, ok := err.(temporary)
	return ok && temp.Temporary()
}

type temporaryErr struct {
	error
	temporary bool
}

func (t *temporaryErr) Temporary() bool {
	return t.temporary
}

// NewTemporaryError creates a new error which satisfies the Temporary interface.
func NewTemporaryError(err error) *temporaryErr {
	return &temporaryErr{error: err, temporary: true}
}
