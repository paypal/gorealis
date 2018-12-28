package realis_test

import (
	"encoding/json"
	"testing"

	"github.com/paypal/gorealis/v2"
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
	thermos := realis.ThermosExecutor{}

	err := json.Unmarshal(thermosJSON, &thermos)

	assert.NoError(t, err)

	thermosExpected := realis.ThermosExecutor{
		Task: realis.ThermosTask{
			Processes: []realis.ThermosProcess{
				{
					Daemon:      false,
					Name:        "hello",
					Ephemeral:   false,
					MaxFailures: 1,
					MinDuration: 5,
					Cmdline:     "\n    while true; do\n      echo hello world from gorealis\n      sleep 10\n    done\n  ",
					Final:       false,
				},
			},
			Constraints: []realis.ThermosConstraint{{Order: []string{"hello"}}},
			Resources:   realis.ThermosResources{CPU: 1.1, Disk: 134217728, RAM: 134217728, GPU: 0}}}

	assert.ObjectsAreEqual(thermosExpected, thermos)
}
