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

func NewActiveQueriesSummaryMetricsReader(
	projectId string,
	instanceId string,
	databaseName string) *MetricsReader {

	query := "select * from spanner_sys.active_queries_summary;"

	// Labels
	var queryLabelValuesMetadata []metadata.LabelValueMetadata

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "active_count",
				MetricColumnName: "ACTIVE_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "count_older_than_1s",
				MetricColumnName: "COUNT_OLDER_THAN_1S",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "count_older_than_10s",
				MetricColumnName: "COUNT_OLDER_THAN_10S",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		metadata.Int64MetricValueMetadata{
			QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
				MetricName:       "count_older_than_100s",
				MetricColumnName: "COUNT_OLDER_THAN_100S",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},
	}

	return &MetricsReader{
		Name:                      "active queries summary",
		ProjectId:                 projectId,
		InstanceId:                instanceId,
		DatabaseName:              databaseName,
		Query:                     query,
		MetricNamePrefix:          "database/spanner/active_queries_summary/",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}
