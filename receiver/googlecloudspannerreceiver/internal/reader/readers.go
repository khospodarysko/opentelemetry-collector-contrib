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
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/statsmetadata"
)

/* ---------- Active Queries Summary Stats ---------------------------------------------------------------------------*/

func NewActiveQueriesSummaryMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource) *MetricsReader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeActiveQueriesSummary)

	return NewMetricsReader(logger, metricsSource, metricsMetadata)
}

/* ---------- Lock Stats ---------------------------------------------------------------------------------------------*/

func NewTopLockStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource,
	topMetricsQueryMaxRows int) *MetricsReader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeLockStatsTop)

	return NewMetricsReaderWithMaxRowsLimit(logger, metricsSource, metricsMetadata, topMetricsQueryMaxRows)
}

func NewTotalLockStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource) *MetricsReader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeLockStatsTotal)

	return NewMetricsReader(logger, metricsSource, metricsMetadata)
}

/* ---------- Query Stats --------------------------------------------------------------------------------------------*/

func NewTopQueryStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource,
	topMetricsQueryMaxRows int) *MetricsReader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeQueryStatsTop)

	return NewMetricsReaderWithMaxRowsLimit(logger, metricsSource, metricsMetadata, topMetricsQueryMaxRows)
}

func NewTotalQueryStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource) *MetricsReader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeQueryStatsTotal)

	return NewMetricsReader(logger, metricsSource, metricsMetadata)
}

/* ---------- Read Stats ---------------------------------------------------------------------------------------------*/

func NewTopReadStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource,
	topMetricsQueryMaxRows int) *MetricsReader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeReadStatsTop)

	return NewMetricsReaderWithMaxRowsLimit(logger, metricsSource, metricsMetadata, topMetricsQueryMaxRows)
}

func NewTotalReadStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource) *MetricsReader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeReadStatsTotal)

	return NewMetricsReader(logger, metricsSource, metricsMetadata)
}

/* ---------- Transaction Stats --------------------------------------------------------------------------------------*/

func NewTopTransactionStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource,
	topMetricsQueryMaxRows int) *MetricsReader {

	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeTransactionStatsTop)

	return NewMetricsReaderWithMaxRowsLimit(logger, metricsSource, metricsMetadata, topMetricsQueryMaxRows)
}

func NewTotalTransactionStatsMetricsReader(logger *zap.Logger, metricsSource *datasource.MetricsSource) *MetricsReader {
	metricsMetadata := statsmetadata.MetricsMetadataHolder().MetricsMetadata(
		statsmetadata.MetricsMetadataTypeTransactionStatsTotal)

	return NewMetricsReader(logger, metricsSource, metricsMetadata)
}
