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

// Using a pattern described by Dave Cheney to differentiate errors
// https://dave.cheney.net/2016/04/27/dont-just-check-errors-handle-them-gracefully
type timeout interface {
	Timeout() bool
}

func IsTimeout(err error) bool {
	temp, ok := err.(timeout)
	return ok && temp.Timeout()
}

type TimeoutErr struct {
	error
	timeout bool
}

func (t *TimeoutErr) Timeout() bool {
	return t.timeout
}

func NewTimeoutError(err error) *TimeoutErr {
	return &TimeoutErr{error: err, timeout: true}
}

type temporary interface {
	Temporary() bool
}

func IsTemporary(err error) bool {
	temp, ok := err.(temporary)
	return ok && temp.Temporary()
}

type TemporaryErr struct {
	error
	temporary bool
}

func (t *TemporaryErr) Temporary() bool {
	return t.temporary
}

// Retrying after receiving this error is advised
func NewTemporaryError(err error) *TemporaryErr {
	return &TemporaryErr{error: err, temporary: true}
}

// Nothing can be done about this error
func NewPermamentError(err error) TemporaryErr {
	return TemporaryErr{error: err, temporary: false}
}
