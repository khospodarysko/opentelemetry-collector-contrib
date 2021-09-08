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
	"fmt"

	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

type ProjectMetricsReader struct {
	databaseMetricsReaders []*DatabaseMetricsReader
	logger                 *zap.Logger
}

func NewProjectMetricsReader(databaseMetricsReaders []*DatabaseMetricsReader, logger *zap.Logger) *ProjectMetricsReader {
	return &ProjectMetricsReader{
		databaseMetricsReaders: databaseMetricsReaders,
		logger:                 logger,
	}
}

func (reader *ProjectMetricsReader) Shutdown() {
	for _, metricsReader := range reader.databaseMetricsReaders {
		reader.logger.Info(fmt.Sprintf("Shutting down metrics reader for database %v", metricsReader.fullDatabaseName))
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
