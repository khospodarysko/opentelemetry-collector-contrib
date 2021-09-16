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

	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
)

type DatabaseReader struct {
	database *datasource.Database
	logger   *zap.Logger
	readers  []Reader
}

func NewDatabaseReader(ctx context.Context,
	databaseId *datasource.DatabaseId,
	serviceAccountPath string,
	readerConfig ReaderConfig,
	logger *zap.Logger) (*DatabaseReader, error) {

	database, err := datasource.NewDatabase(ctx, databaseId, serviceAccountPath)

	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred during client instantiation for database %v", databaseId.Id()))
		return nil, err
	}

	return &DatabaseReader{
		database: database,
		logger:   logger,
		readers: []Reader{
			newTopQueryStatsReader(logger, database, readerConfig),
			newTotalQueryStatsReader(logger, database, readerConfig),
			newTopReadStatsReader(logger, database, readerConfig),
			newTotalReadStatsReader(logger, database, readerConfig),
			newTopTransactionStatsReader(logger, database, readerConfig),
			newTotalTransactionStatsReader(logger, database, readerConfig),
			newTopLockStatsReader(logger, database, readerConfig),
			newTotalLockStatsReader(logger, database, readerConfig),
			newActiveQueriesSummaryReader(logger, database),
		},
	}, nil
}

func (databaseReader *DatabaseReader) Shutdown() {
	databaseReader.logger.Info(fmt.Sprintf("Closing connection to database %v", databaseReader.database.DatabaseId().Id()))
	databaseReader.database.Client().Close()
}

func (databaseReader *DatabaseReader) Read(ctx context.Context) []pdata.Metrics {
	databaseReader.logger.Info(fmt.Sprintf("Executing read method for database %v", databaseReader.database.DatabaseId().Id()))

	var result []pdata.Metrics

	for _, reader := range databaseReader.readers {
		metrics, err := reader.Read(ctx)

		if err != nil {
			databaseReader.logger.Error(fmt.Sprintf("Cannot read data for metrics databaseReader %v because of and error %v",
				reader.Name(), err))
		} else {
			result = append(result, metrics...)
		}
	}

	return result
}
