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

func NewTopQueryStatsMetricsMetadata() *metadata.MetricsMetadata {
	query := "SELECT * FROM SPANNER_SYS.QUERY_STATS_TOP_MINUTE " +
		"WHERE INTERVAL_END = @pullTimestamp " +
		"ORDER BY INTERVAL_END DESC, EXECUTION_COUNT * AVG_CPU_SECONDS DESC"

	// Labels
	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		metadata.StringLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "query_text",
				LabelColumnName: "TEXT",
			},
		},

		metadata.BoolLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "query_text_truncated",
				LabelColumnName: "TEXT_TRUNCATED",
			},
		},

		metadata.Int64LabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "query_text_fingerprint",
				LabelColumnName: "TEXT_FINGERPRINT",
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
				MetricName:       "avg_latency_seconds",
				MetricColumnName: "AVG_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
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
				MetricName:       "avg_rows_scanned",
				MetricColumnName: "AVG_ROWS_SCANNED",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "row",
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

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "all_failed_execution_count",
				MetricColumnName: "ALL_FAILED_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "all_failed_avg_latency_seconds",
				MetricColumnName: "ALL_FAILED_AVG_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "cancelled_or_disconnected_execution_count",
				MetricColumnName: "CANCELLED_OR_DISCONNECTED_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "timed_out_execution_count",
				MetricColumnName: "TIMED_OUT_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},
	}

	return &metadata.MetricsMetadata{
		Name:                      "top minute query stats",
		Query:                     query,
		MetricNamePrefix:          "database/spanner/query_stats/top/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}

func NewTotalQueryStatsMetricsMetadata() *metadata.MetricsMetadata {
	query := "SELECT * FROM spanner_sys.query_stats_total_minute " +
		"WHERE INTERVAL_END = @pullTimestamp"

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
				MetricName:       "avg_latency_seconds",
				MetricColumnName: "AVG_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
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
				MetricName:       "avg_rows_scanned",
				MetricColumnName: "AVG_ROWS_SCANNED",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "row",
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

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "all_failed_execution_count",
				MetricColumnName: "ALL_FAILED_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "all_failed_avg_latency_seconds",
				MetricColumnName: "ALL_FAILED_AVG_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "cancelled_or_disconnected_execution_count",
				MetricColumnName: "CANCELLED_OR_DISCONNECTED_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "timed_out_execution_count",
				MetricColumnName: "TIMED_OUT_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},
	}

	return &metadata.MetricsMetadata{
		Name:                      "total minute query stats",
		Query:                     query,
		MetricNamePrefix:          "database/spanner/query_stats/total/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}
