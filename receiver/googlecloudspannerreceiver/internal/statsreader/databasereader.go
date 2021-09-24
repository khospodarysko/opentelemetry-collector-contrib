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
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

type DatabaseReader struct {
	database *datasource.Database
	logger   *zap.Logger
	readers  []Reader
}

func NewDatabaseReader(ctx context.Context,
	parsedMetadata []*metadata.MetricsMetadata,
	databaseID *datasource.DatabaseID,
	serviceAccountPath string,
	readerConfig ReaderConfig,
	logger *zap.Logger) (*DatabaseReader, error) {

	database, err := datasource.NewDatabase(ctx, databaseID, serviceAccountPath)
	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred during client instantiation for database %v", databaseID.ID()))
		return nil, err
	}

	readers := initializeReaders(logger, parsedMetadata, database, readerConfig)

	return &DatabaseReader{
		database: database,
		logger:   logger,
		readers:  readers,
	}, nil
}

func initializeReaders(logger *zap.Logger, parsedMetadata []*metadata.MetricsMetadata,
	database *datasource.Database, readerConfig ReaderConfig) []Reader {
	var readers []Reader

	for _, mData := range parsedMetadata {
		switch mData.MetadataType() {
		case metadata.MetadataTypeCurrentStats:
			readers = append(readers, newCurrentStatsReader(logger, database, mData, readerConfig))
		case metadata.MetadataTypeIntervalStats:
			readers = append(readers, newIntervalStatsReader(logger, database, mData, readerConfig))
		}
	}

	return readers
}

func (databaseReader *DatabaseReader) Name() string {
	return databaseReader.database.DatabaseID().ID()
}

func (databaseReader *DatabaseReader) Shutdown() {
	databaseReader.logger.Info(fmt.Sprintf("Closing connection to database %v", databaseReader.database.DatabaseID().ID()))
	databaseReader.database.Client().Close()
}

func (databaseReader *DatabaseReader) Read(ctx context.Context) []pdata.Metrics {
	databaseReader.logger.Info(fmt.Sprintf("Executing read method for database %v", databaseReader.database.DatabaseID().ID()))

	var result []pdata.Metrics

	for _, reader := range databaseReader.readers {
		if metrics, err := reader.Read(ctx); err != nil {
			databaseReader.logger.Error(fmt.Sprintf("Cannot read data for metrics databaseReader %v because of and error %v",
				reader.Name(), err))
		} else {
			result = append(result, metrics...)
		}
	}

	return result
}
