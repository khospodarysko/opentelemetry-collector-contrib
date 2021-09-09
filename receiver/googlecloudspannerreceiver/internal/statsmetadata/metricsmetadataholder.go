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

package statsmetadata

import (
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

type MetricsMetadataType int32

const (
	MetricsMetadataTypeQueryStatsTop MetricsMetadataType = iota
	MetricsMetadataTypeQueryStatsTotal
	MetricsMetadataTypeReadStatsTop
	MetricsMetadataTypeReadStatsTotal
	MetricsMetadataTypeTransactionStatsTop
	MetricsMetadataTypeTransactionStatsTotal
	MetricsMetadataTypeLockStatsTop
	MetricsMetadataTypeLockStatsTotal
	MetricsMetadataTypeActiveQueriesSummary
)

type metricsMetadataHolder struct {
	holder map[MetricsMetadataType]*metadata.MetricsMetadata
}

func newMetricsMetadataHolder() *metricsMetadataHolder {
	holder := make(map[MetricsMetadataType]*metadata.MetricsMetadata)

	holder[MetricsMetadataTypeQueryStatsTop] = NewTopQueryStatsMetricsMetadata()
	holder[MetricsMetadataTypeQueryStatsTotal] = NewTotalQueryStatsMetricsMetadata()

	holder[MetricsMetadataTypeReadStatsTop] = NewTopReadStatsMetricsMetadata()
	holder[MetricsMetadataTypeReadStatsTotal] = NewTotalReadStatsMetricsMetadata()

	holder[MetricsMetadataTypeTransactionStatsTop] = NewTopTransactionStatsMetricsMetadata()
	holder[MetricsMetadataTypeTransactionStatsTotal] = NewTotalTransactionStatsMetricsMetadata()

	holder[MetricsMetadataTypeLockStatsTop] = NewTopLockStatsMetricsMetadata()
	holder[MetricsMetadataTypeLockStatsTotal] = NewTotalLockStatsMetricsMetadata()

	holder[MetricsMetadataTypeActiveQueriesSummary] = NewActiveQueriesSummaryMetricsMetadata()

	return &metricsMetadataHolder{
		holder: holder,
	}
}

func (h *metricsMetadataHolder) MetricsMetadata(metricsMetadataType MetricsMetadataType) *metadata.MetricsMetadata {
	return h.holder[metricsMetadataType]
}

var mrmHolder *metricsMetadataHolder
var once sync.Once

func MetricsMetadataHolder() *metricsMetadataHolder {
	once.Do(func() {
		mrmHolder = newMetricsMetadataHolder()
	})

	return mrmHolder
}
