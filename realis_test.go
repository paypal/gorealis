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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCACerts(t *testing.T) {
	certs, err := GetCerts("./examples/certs")
	require.NoError(t, err)
	assert.Equal(t, len(certs.Subjects()), 2)
}

func TestAuroraURLValidator(t *testing.T) {
	t.Run("badURL", func(t *testing.T) {
		url, err := validateAuroraAddress("http://badurl.com/badpath")
		assert.Empty(t, url)
		assert.Error(t, err)
	})

	t.Run("URLHttp", func(t *testing.T) {
		url, err := validateAuroraAddress("http://goodurl.com:8081/api")
		assert.Equal(t, "http://goodurl.com:8081/api", url)
		assert.NoError(t, err)
	})

	t.Run("URLHttps", func(t *testing.T) {
		url, err := validateAuroraAddress("https://goodurl.com:8081/api")
		assert.Equal(t, "https://goodurl.com:8081/api", url)
		assert.NoError(t, err)
	})

	t.Run("URLNoPath", func(t *testing.T) {
		url, err := validateAuroraAddress("http://goodurl.com:8081")
		assert.Equal(t, "http://goodurl.com:8081/api", url)
		assert.NoError(t, err)
	})

	t.Run("ipAddrNoPath", func(t *testing.T) {
		url, err := validateAuroraAddress("http://192.168.1.33:8081")
		assert.Equal(t, "http://192.168.1.33:8081/api", url)
		assert.NoError(t, err)
	})

	t.Run("URLNoProtocol", func(t *testing.T) {
		url, err := validateAuroraAddress("goodurl.com:8081/api")
		assert.Equal(t, "http://goodurl.com:8081/api", url)
		assert.NoError(t, err)
	})

	t.Run("URLNoProtocolNoPathNoPort", func(t *testing.T) {
		url, err := validateAuroraAddress("goodurl.com")
		assert.Equal(t, "http://goodurl.com:8081/api", url)
		assert.NoError(t, err)
	})
}
