package realis

import (
	"encoding/json"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

func TestThermosTask(t *testing.T) {

	// Test that we can successfully deserialize a minimum subset of an Aurora generated thermos payload
	thermosJSON := []byte(
		`{
  "task": {
    "processes": [
      {
        "daemon": false,
        "name": "hello",
        "ephemeral": false,
        "max_failures": 1,
        "min_duration": 5,
        "cmdline": "\n    while true; do\n      echo hello world from gorealis\n      sleep 10\n    done\n  ",
        "final": false
      }
    ],
    "resources": {
      "gpu": 0,
      "disk": 134217728,
      "ram": 134217728,
      "cpu": 1.1
    },
    "constraints": [
      {
        "order": [
          "hello"
        ]
      }
    ]
  }
}`)
	thermos := ThermosExecutor{}

	err := json.Unmarshal(thermosJSON, &thermos)

	assert.NoError(t, err)

	process := &ThermosProcess{
		Daemon:      false,
		Name:        "hello",
		Ephemeral:   false,
		MaxFailures: 1,
		MinDuration: 5,
		Cmdline:     "\n    while true; do\n      echo hello world from gorealis\n      sleep 10\n    done\n  ",
		Final:       false,
	}

	constraint := &ThermosConstraint{Order: []string{process.Name}}

	thermosExpected := ThermosExecutor{
		Task: ThermosTask{
			Processes:   map[string]*ThermosProcess{process.Name: process},
			Constraints: []*ThermosConstraint{constraint},
			Resources: thermosResources{CPU: thrift.Float64Ptr(1.1),
				Disk: thrift.Int64Ptr(134217728),
				RAM:  thrift.Int64Ptr(134217728),
				GPU:  thrift.Int64Ptr(0)}}}

	assert.ObjectsAreEqualValues(thermosExpected, thermos)
}
