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

	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

type ProjectMetricsReader struct {
	databaseMetricsReaders map[string]*DatabaseMetricsReader
	project                Project
	logger                 *zap.Logger
}

func NewProjectMetricsReader(project Project, ctx context.Context, logger *zap.Logger) (*ProjectMetricsReader, error) {
	logger.Info(fmt.Sprintf("Constructing project metrics reader for project id %v", project.ID))

	databaseMetricsReaders := make(map[string]*DatabaseMetricsReader)

	for _, instance := range project.Instances {
		for _, database := range instance.Databases {
			logger.Info(fmt.Sprintf("Constructing database metrics reader for project id %v, instance id %v, database %v",
				project.ID, instance.ID, database.Name))

			databaseMetricsReader, err := NewDatabaseMetricsReader(ctx, project.ID, instance.ID, database.Name,
				project.ServiceAccountKey, logger)

			if err != nil {
				return nil, err
			}

			databaseMetricsReaders[databaseMetricsReader.fullDatabaseName] = databaseMetricsReader
		}
	}

	return &ProjectMetricsReader{databaseMetricsReaders: databaseMetricsReaders, project: project, logger: logger}, nil
}

func (reader *ProjectMetricsReader) Shutdown() {
	for database, metricsReader := range reader.databaseMetricsReaders {
		reader.logger.Info(fmt.Sprintf("Shutting down metrics reader for database %v", database))
		metricsReader.Shutdown()
	}
}

func (reader *ProjectMetricsReader) ReadMetrics(ctx context.Context) []pdata.Metrics {
	var projectMetrics []pdata.Metrics

	for _, databaseMetricsReader := range reader.databaseMetricsReaders {
		projectMetrics = append(projectMetrics, databaseMetricsReader.ReadMetrics(ctx)...)
	}

	return projectMetrics
}
