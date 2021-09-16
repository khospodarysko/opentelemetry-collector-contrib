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
	"fmt"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

type currentStatsReader struct {
	logger                 *zap.Logger
	database               *datasource.Database
	metricsMetadata        *metadata.MetricsMetadata
	topMetricsQueryMaxRows int
	statement              func(args statementArgs) spanner.Statement
}

func newCurrentStatsReaderWithMaxRowsLimit(
	logger *zap.Logger,
	database *datasource.Database,
	metricsMetadata *metadata.MetricsMetadata,
	topMetricsQueryMaxRows int) *currentStatsReader {

	return &currentStatsReader{
		logger:                 logger,
		database:               database,
		metricsMetadata:        metricsMetadata,
		topMetricsQueryMaxRows: topMetricsQueryMaxRows,
		statement:              currentStatsStatement,
	}
}

func newCurrentStatsReader(
	logger *zap.Logger,
	database *datasource.Database,
	metricsMetadata *metadata.MetricsMetadata) *currentStatsReader {

	return &currentStatsReader{
		logger:          logger,
		database:        database,
		metricsMetadata: metricsMetadata,
		statement:       currentStatsStatement,
	}
}

func (reader *currentStatsReader) Name() string {
	return reader.metricsMetadata.Name + " " +
		reader.database.DatabaseId().ProjectId() + "::" +
		reader.database.DatabaseId().InstanceId() + "::" +
		reader.database.DatabaseId().DatabaseName()
}

func (reader *currentStatsReader) Read(ctx context.Context) ([]pdata.Metrics, error) {
	reader.logger.Info(fmt.Sprintf("Executing read method for reader %v", reader.Name()))

	stmtArgs := statementArgs{
		query:                  reader.metricsMetadata.Query,
		topMetricsQueryMaxRows: reader.topMetricsQueryMaxRows,
	}

	stmt := reader.statement(stmtArgs)

	return reader.pull(ctx, stmt)
}

func (reader *currentStatsReader) pull(ctx context.Context, stmt spanner.Statement) ([]pdata.Metrics, error) {
	rowsIterator := reader.database.Client().Single().Query(ctx, stmt)
	defer rowsIterator.Stop()

	var collectedMetrics []pdata.Metrics

	for {
		row, err := rowsIterator.Next()

		if err != nil {
			if err == iterator.Done {
				break
			}

			reader.logger.Error(fmt.Sprintf("Query \"%v\" failed with %v", stmt.SQL, err))

			return nil, err
		}

		rowMetrics, err := reader.metricsMetadata.RowToMetrics(reader.database.DatabaseId(), row)

		if err != nil {
			reader.logger.Error(fmt.Sprintf("Query \"%v\" failed with %v", stmt.SQL, err))
			return nil, err
		}

		collectedMetrics = append(collectedMetrics, rowMetrics...)
	}

	return collectedMetrics, nil
}
