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

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	topMetricsQueryLimitParameterName = "topMetricsQueryMaxRows"
	topMetricsQueryLimitCondition     = " LIMIT @" + topMetricsQueryLimitParameterName
)

type MetricsReader struct {
	logger                 *zap.Logger
	metricsSource          *datasource.MetricsSource
	metricsMetadata        *metadata.MetricsMetadata
	topMetricsQueryMaxRows int
}

func NewMetricsReaderWithMaxRowsLimit(
	logger *zap.Logger,
	metricsSource *datasource.MetricsSource,
	metricsMetadata *metadata.MetricsMetadata,
	topMetricsQueryMaxRows int) *MetricsReader {

	return &MetricsReader{
		logger:                 logger,
		metricsSource:          metricsSource,
		metricsMetadata:        metricsMetadata,
		topMetricsQueryMaxRows: topMetricsQueryMaxRows,
	}
}

func NewMetricsReader(
	logger *zap.Logger,
	metricsSource *datasource.MetricsSource,
	metricsMetadata *metadata.MetricsMetadata) *MetricsReader {

	return &MetricsReader{
		logger:          logger,
		metricsSource:   metricsSource,
		metricsMetadata: metricsMetadata,
	}
}

func (metricsReader *MetricsReader) Name() string {
	return metricsReader.metricsMetadata.Name + " " +
		metricsReader.metricsSource.MetricsSourceId().ProjectId() + "::" +
		metricsReader.metricsSource.MetricsSourceId().InstanceId() + "::" +
		metricsReader.metricsSource.MetricsSourceId().DatabaseName()
}

func (metricsReader *MetricsReader) Read(ctx context.Context) ([]pdata.Metrics, error) {
	metricsReader.logger.Info(fmt.Sprintf("Executing read method for metrics reader %v", metricsReader.Name()))

	stmt := buildStatement(metricsReader.metricsMetadata.Query, metricsReader.topMetricsQueryMaxRows)

	rowsIterator := metricsReader.metricsSource.Client().Single().Query(ctx, stmt)
	defer rowsIterator.Stop()

	var collectedMetrics []pdata.Metrics

	for {
		row, err := rowsIterator.Next()

		if err != nil {
			if err == iterator.Done {
				break
			}

			metricsReader.logger.Error(fmt.Sprintf("Query \"%v\" failed with %v", stmt.SQL, err))

			return nil, err
		}

		rowMetrics, err := metricsReader.metricsMetadata.RowToMetrics(metricsReader.metricsSource.MetricsSourceId(), row)

		if err != nil {
			metricsReader.logger.Error(fmt.Sprintf("Query \"%v\" failed with %v", stmt.SQL, err))
			return nil, err
		}

		collectedMetrics = append(collectedMetrics, rowMetrics...)
	}

	return collectedMetrics, nil
}

func buildStatement(query string, topMetricsQueryMaxRows int) spanner.Statement {
	stmt := spanner.Statement{SQL: query}

	if topMetricsQueryMaxRows > 0 {
		stmt = spanner.Statement{
			SQL: query + topMetricsQueryLimitCondition,
			Params: map[string]interface{}{
				topMetricsQueryLimitParameterName: topMetricsQueryMaxRows,
			},
		}
	}

	return stmt
}
