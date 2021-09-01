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

package reader

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

	query = "query"

	topMetricsQueryMaxRows = 10
)

func TestMetricsReader_Name(t *testing.T) {
	metricsSourceId := datasource.NewMetricsSourceId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	metricsSource := datasource.NewMetricsSourceFromClient(client, metricsSourceId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	metricsReader := MetricsReader{
		metricsSource:   metricsSource,
		metricsMetadata: metricsMetadata,
	}

	assert.Equal(t, metricsReader.metricsMetadata.Name+" "+metricsSourceId.ProjectId()+"::"+
		metricsSourceId.InstanceId()+"::"+metricsSourceId.DatabaseName(), metricsReader.Name())
}

func TestNewMetricsReaderWithMaxRowsLimit(t *testing.T) {
	metricsSourceId := datasource.NewMetricsSourceId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	metricsSource := datasource.NewMetricsSourceFromClient(client, metricsSourceId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	logger := zap.NewNop()

	metricsReader := NewMetricsReaderWithMaxRowsLimit(logger, metricsSource, metricsMetadata, topMetricsQueryMaxRows)

	assert.Equal(t, metricsSource, metricsReader.metricsSource)
	assert.Equal(t, logger, metricsReader.logger)
	assert.Equal(t, metricsMetadata, metricsReader.metricsMetadata)
	assert.Equal(t, topMetricsQueryMaxRows, metricsReader.topMetricsQueryMaxRows)
}

func TestNewMetricsReader(t *testing.T) {
	metricsSourceId := datasource.NewMetricsSourceId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	metricsSource := datasource.NewMetricsSourceFromClient(client, metricsSourceId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	logger := zap.NewNop()

	metricsReader := NewMetricsReader(logger, metricsSource, metricsMetadata)

	assert.Equal(t, metricsSource, metricsReader.metricsSource)
	assert.Equal(t, logger, metricsReader.logger)
	assert.Equal(t, metricsMetadata, metricsReader.metricsMetadata)
	assert.Equal(t, 0, metricsReader.topMetricsQueryMaxRows)
}

func TestBuildStatement(t *testing.T) {
	statement := buildStatement(query, topMetricsQueryMaxRows)

	assert.Equal(t, query+topMetricsQueryLimitCondition, statement.SQL)
	assert.Equal(t, map[string]interface{}{topMetricsQueryLimitParameterName: topMetricsQueryMaxRows}, statement.Params)

	statement = buildStatement(query, 0)

	assert.Equal(t, query, statement.SQL)
	assert.Nil(t, statement.Params)
}
