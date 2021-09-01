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

func NewTopReadStatsMetricsMetadata() *metadata.MetricsMetadata {
	query := "SELECT * FROM spanner_sys.read_stats_top_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.read_stats_top_minute)" +
		"ORDER BY EXECUTION_COUNT * AVG_CPU_SECONDS DESC"

	// Labels
	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		metadata.StringSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "read_columns",
				LabelColumnName: "READ_COLUMNS",
			},
		},

		metadata.Int64LabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "fingerprint",
				LabelColumnName: "FPRINT",
			},
		},
	}

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "execution_count",
				MetricColumnName: "EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_rows",
				MetricColumnName: "AVG_ROWS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "row",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_bytes",
				MetricColumnName: "AVG_BYTES",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "byte",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_cpu_seconds",
				MetricColumnName: "AVG_CPU_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_locking_delay_seconds",
				MetricColumnName: "AVG_LOCKING_DELAY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_client_wait_seconds",
				MetricColumnName: "AVG_CLIENT_WAIT_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_leader_refresh_delay_seconds",
				MetricColumnName: "AVG_LEADER_REFRESH_DELAY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},
	}

	return &metadata.MetricsMetadata{
		Name:                      "top minute read stats",
		Query:                     query,
		MetricNamePrefix:          "database/spanner/read_stats/top/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}

func NewTotalReadStatsMetricsMetadata() *metadata.MetricsMetadata {
	query := "SELECT * FROM spanner_sys.read_stats_total_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.read_stats_total_minute)"

	// Labels
	var queryLabelValuesMetadata []metadata.LabelValueMetadata

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "execution_count",
				MetricColumnName: "EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_rows",
				MetricColumnName: "AVG_ROWS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "row",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_bytes",
				MetricColumnName: "AVG_BYTES",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "byte",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_cpu_seconds",
				MetricColumnName: "AVG_CPU_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_locking_delay_seconds",
				MetricColumnName: "AVG_LOCKING_DELAY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_client_wait_seconds",
				MetricColumnName: "AVG_CLIENT_WAIT_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_leader_refresh_delay_seconds",
				MetricColumnName: "AVG_LEADER_REFRESH_DELAY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},
	}

	return &metadata.MetricsMetadata{
		Name:                      "total minute read stats",
		Query:                     query,
		MetricNamePrefix:          "database/spanner/read_stats/total/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}
