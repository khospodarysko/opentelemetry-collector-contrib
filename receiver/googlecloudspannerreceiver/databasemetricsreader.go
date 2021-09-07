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

package googlecloudspannerreceiver

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

type DatabaseMetricsReader struct {
	client                *spanner.Client
	fullDatabaseName      string
	logger                *zap.Logger
	metricsReaderMetadata []*MetricsReaderMetadata
}

func NewDatabaseMetricsReader(ctx context.Context, projectId string, instanceId string, databaseName string,
	serviceAccountPath string, topMetricsQueryMaxRows int, logger *zap.Logger) (*DatabaseMetricsReader, error) {
	fullDatabaseName := createFullDatabaseName(projectId, instanceId, databaseName)
	client, err := createClient(ctx, fullDatabaseName, serviceAccountPath, logger)

	if err != nil {
		return nil, err
	}

	return &DatabaseMetricsReader{
		client:           client,
		fullDatabaseName: fullDatabaseName,
		logger:           logger,
		metricsReaderMetadata: []*MetricsReaderMetadata{
			NewTopQueryStatsMetricsReaderMetadata(projectId, instanceId, databaseName, topMetricsQueryMaxRows),
			NewTotalQueryStatsMetricsReaderMetadata(projectId, instanceId, databaseName),
			NewTopReadStatsMetricsReaderMetadata(projectId, instanceId, databaseName, topMetricsQueryMaxRows),
			NewTopTransactionStatsMetricsReaderMetadata(projectId, instanceId, databaseName, topMetricsQueryMaxRows),
		},
	}, nil
}

func createClient(ctx context.Context, database string, serviceAccountPath string, logger *zap.Logger) (*spanner.Client, error) {
	serviceAccountClientOption := option.WithCredentialsFile(serviceAccountPath)
	client, err := spanner.NewClient(ctx, database, serviceAccountClientOption)

	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred during client instantiation for database %v", database))
		return nil, err
	}

	return client, nil
}

func createFullDatabaseName(projectId string, instanceId string, database string) string {
	return "projects/" + projectId + "/instances/" + instanceId + "/databases/" + database
}

func (reader *DatabaseMetricsReader) Shutdown() {
	reader.logger.Info(fmt.Sprintf("Closing connection to database %v", reader.fullDatabaseName))
	reader.client.Close()
}

func (reader *DatabaseMetricsReader) ReadMetrics(ctx context.Context) []pdata.Metrics {
	reader.logger.Info(fmt.Sprintf("Executing read method for database %v", reader.fullDatabaseName))

	var result []pdata.Metrics

	for _, metadata := range reader.metricsReaderMetadata {
		metrics, err := metadata.ReadMetrics(ctx, reader.client, reader.logger)

		if err != nil {
			reader.logger.Error(fmt.Sprintf("Cannot read data for metrics metadata %v because of and error %v", metadata.Name, err))
		} else {
			result = append(result, metrics...)
		}
	}

	return result
}
