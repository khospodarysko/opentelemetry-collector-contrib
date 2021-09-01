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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
)

type DatabaseMetricsReader struct {
	metricsSource  *datasource.MetricsSource
	logger         *zap.Logger
	metricsReaders []*MetricsReader
}

func NewDatabaseMetricsReader(ctx context.Context,
	metricsSourceId *datasource.MetricsSourceId,
	serviceAccountPath string,
	topMetricsQueryMaxRows int,
	logger *zap.Logger) (*DatabaseMetricsReader, error) {

	metricsSource, err := datasource.NewMetricsSource(ctx, metricsSourceId, serviceAccountPath)

	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred during client instantiation for database %v", metricsSourceId.Id()))
		return nil, err
	}

	return &DatabaseMetricsReader{
		metricsSource: metricsSource,
		logger:        logger,
		metricsReaders: []*MetricsReader{
			NewTopQueryStatsMetricsReader(logger, metricsSource, topMetricsQueryMaxRows),
			NewTotalQueryStatsMetricsReader(logger, metricsSource),
			NewTopReadStatsMetricsReader(logger, metricsSource, topMetricsQueryMaxRows),
			NewTotalReadStatsMetricsReader(logger, metricsSource),
			NewTopTransactionStatsMetricsReader(logger, metricsSource, topMetricsQueryMaxRows),
			NewTotalTransactionStatsMetricsReader(logger, metricsSource),
			NewTopLockStatsMetricsReader(logger, metricsSource, topMetricsQueryMaxRows),
			NewTotalLockStatsMetricsReader(logger, metricsSource),
			NewActiveQueriesSummaryMetricsReader(logger, metricsSource),
		},
	}, nil
}

func (reader *DatabaseMetricsReader) Shutdown() {
	reader.logger.Info(fmt.Sprintf("Closing connection to database %v", reader.metricsSource.MetricsSourceId().Id()))
	reader.metricsSource.Client().Close()
}

func (reader *DatabaseMetricsReader) ReadMetrics(ctx context.Context) []pdata.Metrics {
	reader.logger.Info(fmt.Sprintf("Executing read method for database %v", reader.metricsSource.MetricsSourceId().Id()))

	var result []pdata.Metrics

	for _, metricsReader := range reader.metricsReaders {
		metrics, err := metricsReader.Read(ctx)

		if err != nil {
			reader.logger.Error(fmt.Sprintf("Cannot read data for metrics reader %v because of and error %v",
				metricsReader.Name(), err))
		} else {
			result = append(result, metrics...)
		}
	}

	return result
}
