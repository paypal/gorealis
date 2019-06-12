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

type logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
	Print(v ...interface{})
}

// NoopLogger is a logger that can be attached to the client which will not print anything.
type NoopLogger struct{}

// Printf is a NOOP function here.
func (NoopLogger) Printf(format string, a ...interface{}) {}

// Print is a NOOP function here.
func (NoopLogger) Print(a ...interface{}) {}

// Println is a NOOP function here.
func (NoopLogger) Println(a ...interface{}) {}

// LevelLogger is a logger that can be configured to output different levels of information: Debug and Trace.
// Trace should only be enabled when very in depth information about the sequence of events a function took is needed.
type LevelLogger struct {
	logger
	debug bool
	trace bool
}

// EnableDebug enables debug level logging for the LevelLogger
func (l *LevelLogger) EnableDebug(enable bool) {
	l.debug = enable
}

// EnableTrace enables trace level logging for the LevelLogger
func (l *LevelLogger) EnableTrace(enable bool) {
	l.trace = enable
}

func (l LevelLogger) debugPrintf(format string, a ...interface{}) {
	if l.debug {
		l.Printf("[DEBUG] "+format, a...)
	}
}

func (l LevelLogger) debugPrint(a ...interface{}) {
	if l.debug {
		l.Print(append([]interface{}{"[DEBUG] "}, a...)...)
	}
}

func (l LevelLogger) debugPrintln(a ...interface{}) {
	if l.debug {
		l.Println(append([]interface{}{"[DEBUG] "}, a...)...)
	}
}

func (l LevelLogger) tracePrintf(format string, a ...interface{}) {
	if l.trace {
		l.Printf("[TRACE] "+format, a...)
	}
}

func (l LevelLogger) tracePrint(a ...interface{}) {
	if l.trace {
		l.Print(append([]interface{}{"[TRACE] "}, a...)...)
	}
}

func (l LevelLogger) tracePrintln(a ...interface{}) {
	if l.trace {
		l.Println(append([]interface{}{"[TRACE] "}, a...)...)
	}
}
