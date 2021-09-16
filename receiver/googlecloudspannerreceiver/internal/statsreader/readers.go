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
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/statsmetadata"
)

/* ---------- Active Queries Summary Stats ---------------------------------------------------------------------------*/

func newActiveQueriesSummaryReader(logger *zap.Logger, database *datasource.Database) Reader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeActiveQueriesSummary)

	return newCurrentStatsReader(logger, database, metricsMetadata)
}

/* ---------- Lock Stats ---------------------------------------------------------------------------------------------*/

func newTopLockStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeLockStatsTop)

	return newIntervalStatsReaderWithMaxRowsLimit(logger, database, metricsMetadata,
		config.BackFillEnabled, config.TopMetricsQueryMaxRows)
}

func newTotalLockStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeLockStatsTotal)

	return newIntervalStatsReader(logger, database, metricsMetadata, config.BackFillEnabled)
}

/* ---------- Query Stats --------------------------------------------------------------------------------------------*/

func newTopQueryStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeQueryStatsTop)

	return newIntervalStatsReaderWithMaxRowsLimit(logger, database, metricsMetadata,
		config.BackFillEnabled, config.TopMetricsQueryMaxRows)
}

func newTotalQueryStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeQueryStatsTotal)

	return newIntervalStatsReader(logger, database, metricsMetadata, config.BackFillEnabled)
}

/* ---------- Read Stats ---------------------------------------------------------------------------------------------*/

func newTopReadStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeReadStatsTop)

	return newIntervalStatsReaderWithMaxRowsLimit(logger, database, metricsMetadata,
		config.BackFillEnabled, config.TopMetricsQueryMaxRows)
}

func newTotalReadStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeReadStatsTotal)

	return newIntervalStatsReader(logger, database, metricsMetadata, config.BackFillEnabled)
}

/* ---------- Transaction Stats --------------------------------------------------------------------------------------*/

func newTopTransactionStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeTransactionStatsTop)

	return newIntervalStatsReaderWithMaxRowsLimit(logger, database, metricsMetadata,
		config.BackFillEnabled, config.TopMetricsQueryMaxRows)
}

func newTotalTransactionStatsReader(logger *zap.Logger, database *datasource.Database, config ReaderConfig) Reader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeTransactionStatsTotal)

	return newIntervalStatsReader(logger, database, metricsMetadata, config.BackFillEnabled)
}
