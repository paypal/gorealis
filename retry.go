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

	"github.com/pkg/errors"
)

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
