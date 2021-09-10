// Copyright  OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datasource

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	projectId    = "ProjectId"
	instanceId   = "InstanceId"
	databaseName = "DatabaseName"
)

func metricsSourceId() *MetricsSourceId {
	return NewMetricsSourceId(projectId, instanceId, databaseName)
}

func TestNewMetricsSourceId(t *testing.T) {
	metricsSourceId := metricsSourceId()

	assert.Equal(t, projectId, metricsSourceId.projectId)
	assert.Equal(t, instanceId, metricsSourceId.instanceId)
	assert.Equal(t, databaseName, metricsSourceId.databaseName)
	assert.Equal(t, "projects/"+projectId+"/instances/"+instanceId+"/databases/"+databaseName, metricsSourceId.id)
	assert.Equal(t, projectId, metricsSourceId.ProjectId())
	assert.Equal(t, instanceId, metricsSourceId.InstanceId())
	assert.Equal(t, databaseName, metricsSourceId.DatabaseName())
	assert.Equal(t, "projects/"+projectId+"/instances/"+instanceId+"/databases/"+databaseName, metricsSourceId.Id())
}
