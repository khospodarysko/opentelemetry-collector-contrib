// Copyright  The OpenTelemetry Authors
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

package metadata

import (
	"go.opentelemetry.io/collector/model/pdata"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
)

type MetricsDataPointGroupingKey struct {
	MetricName     string
	MetricUnit     string
	MetricDataType MetricDataType
}

type MetricsDataPoint struct {
	metricName  string
	timestamp   time.Time
	databaseID  *datasource.DatabaseID
	labelValues []LabelValue
	metricValue MetricValue
}

func (mdp *MetricsDataPoint) CopyToNumberDataPoint(point pdata.NumberDataPoint) {
	point.SetTimestamp(pdata.NewTimestampFromTime(mdp.timestamp))

	mdp.metricValue.SetValueTo(point)

	attributes := point.Attributes()

	attributes.InsertString(projectIDLabelName, mdp.databaseID.ProjectID())
	attributes.InsertString(instanceIDLabelName, mdp.databaseID.InstanceID())
	attributes.InsertString(databaseLabelName, mdp.databaseID.DatabaseName())

	for _, labelValue := range mdp.labelValues {
		labelValue.SetAttributesTo(attributes)
	}
}

func (mdp *MetricsDataPoint) GroupingKey() MetricsDataPointGroupingKey {
	return MetricsDataPointGroupingKey{
		MetricName:     mdp.metricName,
		MetricUnit:     mdp.metricValue.Unit(),
		MetricDataType: mdp.metricValue.DataType(),
	}
}
