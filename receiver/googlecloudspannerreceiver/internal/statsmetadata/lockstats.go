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
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

func NewTopLockStatsMetricsMetadata() *metadata.MetricsMetadata {
	query := "SELECT * FROM spanner_sys.lock_stats_top_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.lock_stats_top_minute)" +
		"ORDER BY LOCK_WAIT_SECONDS DESC"

	// Labels
	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		metadata.ByteSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "row_range_start_key",
				LabelColumnName: "ROW_RANGE_START_KEY",
			},
		},
	}

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "lock_wait_seconds",
				MetricColumnName: "LOCK_WAIT_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},
	}

	return &metadata.MetricsMetadata{
		Name:                      "top minute lock stats",
		Query:                     query,
		MetricNamePrefix:          "database/spanner/lock_stats/top/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}

func NewTotalLockStatsMetricsMetadata() *metadata.MetricsMetadata {
	query := "SELECT * FROM spanner_sys.lock_stats_total_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.lock_stats_total_minute)"

	// Labels
	var queryLabelValuesMetadata []metadata.LabelValueMetadata

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "total_lock_wait_seconds",
				MetricColumnName: "TOTAL_LOCK_WAIT_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},
	}

	return &metadata.MetricsMetadata{
		Name:                      "total minute lock stats",
		Query:                     query,
		MetricNamePrefix:          "database/spanner/lock_stats/total/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}
