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
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

func NewTopTransactionStatsMetricsReader(
	projectId string,
	instanceId string,
	databaseName string,
	topMetricsQueryMaxRows int) *MetricsReader {

	query := "SELECT * FROM spanner_sys.txn_stats_top_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.txn_stats_top_minute)" +
		"ORDER BY AVG_COMMIT_LATENCY_SECONDS DESC, COMMIT_ATTEMPT_COUNT DESC, AVG_BYTES DESC"

	// Labels
	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		metadata.Int64LabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "fingerprint",
				LabelColumnName: "FPRINT",
			},
		},

		metadata.StringSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "read_columns",
				LabelColumnName: "READ_COLUMNS",
			},
		},

		metadata.StringSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "write_constructive_columns",
				LabelColumnName: "WRITE_CONSTRUCTIVE_COLUMNS",
			},
		},

		metadata.StringSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "write_delete_tables",
				LabelColumnName: "WRITE_DELETE_TABLES",
			},
		},
	}

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_attempt_count",
				MetricColumnName: "COMMIT_ATTEMPT_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_abort_count",
				MetricColumnName: "COMMIT_ABORT_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_retry_count",
				MetricColumnName: "COMMIT_RETRY_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_failed_precondition_count",
				MetricColumnName: "COMMIT_FAILED_PRECONDITION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_participants",
				MetricColumnName: "AVG_PARTICIPANTS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_total_latency_seconds",
				MetricColumnName: "AVG_TOTAL_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_commit_latency_seconds",
				MetricColumnName: "AVG_COMMIT_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
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
	}

	return &MetricsReader{
		Name:                      "top minute transaction stats",
		ProjectId:                 projectId,
		InstanceId:                instanceId,
		DatabaseName:              databaseName,
		Query:                     query,
		TopMetricsQueryMaxRows:    topMetricsQueryMaxRows,
		MetricNamePrefix:          "database/spanner/txn_stats/top/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}

func NewTotalTransactionStatsMetricsReader(
	projectId string,
	instanceId string,
	databaseName string) *MetricsReader {

	query := "SELECT * FROM spanner_sys.txn_stats_total_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.txn_stats_total_minute)"

	// Labels
	var queryLabelValuesMetadata []metadata.LabelValueMetadata

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_attempt_count",
				MetricColumnName: "COMMIT_ATTEMPT_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_abort_count",
				MetricColumnName: "COMMIT_ABORT_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_retry_count",
				MetricColumnName: "COMMIT_RETRY_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "commit_failed_precondition_count",
				MetricColumnName: "COMMIT_FAILED_PRECONDITION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_participants",
				MetricColumnName: "AVG_PARTICIPANTS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_total_latency_seconds",
				MetricColumnName: "AVG_TOTAL_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
			},
		},

		metadata.Float64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "avg_commit_latency_seconds",
				MetricColumnName: "AVG_COMMIT_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "second",
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
	}

	return &MetricsReader{
		Name:                      "total minute transaction stats",
		ProjectId:                 projectId,
		InstanceId:                instanceId,
		DatabaseName:              databaseName,
		Query:                     query,
		MetricNamePrefix:          "database/spanner/txn_stats/total/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}
