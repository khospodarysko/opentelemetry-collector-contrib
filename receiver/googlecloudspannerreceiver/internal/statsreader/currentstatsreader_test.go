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

package statsreader

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	projectId    = "ProjectId"
	instanceId   = "InstanceId"
	databaseName = "DatabaseName"
)

func TestCurrentStatsReader_Name(t *testing.T) {
	databaseId := datasource.NewDatabaseId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	database := datasource.NewDatabaseFromClient(client, databaseId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	reader := currentStatsReader{
		database:        database,
		metricsMetadata: metricsMetadata,
	}

	assert.Equal(t, reader.metricsMetadata.Name+" "+databaseId.ProjectId()+"::"+
		databaseId.InstanceId()+"::"+databaseId.DatabaseName(), reader.Name())
}

func TestNewCurrentStatsReaderWithMaxRowsLimit(t *testing.T) {
	databaseId := datasource.NewDatabaseId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	database := datasource.NewDatabaseFromClient(client, databaseId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	logger := zap.NewNop()

	reader := newCurrentStatsReaderWithMaxRowsLimit(logger, database, metricsMetadata, topMetricsQueryMaxRows)

	assert.Equal(t, database, reader.database)
	assert.Equal(t, logger, reader.logger)
	assert.Equal(t, metricsMetadata, reader.metricsMetadata)
	assert.Equal(t, topMetricsQueryMaxRows, reader.topMetricsQueryMaxRows)
}

func TestNewCurrentStatsReader(t *testing.T) {
	databaseId := datasource.NewDatabaseId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	database := datasource.NewDatabaseFromClient(client, databaseId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	logger := zap.NewNop()

	reader := newCurrentStatsReader(logger, database, metricsMetadata)

	assert.Equal(t, database, reader.database)
	assert.Equal(t, logger, reader.logger)
	assert.Equal(t, metricsMetadata, reader.metricsMetadata)
	assert.Equal(t, 0, reader.topMetricsQueryMaxRows)
}
