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

package metadataparser

import (
	"fmt"

	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	metricDataTypeGauge = "gauge"
	metricDataTypeSum   = "sum"

	metricValueTypeInt   = "int"
	metricValueTypeFloat = "float"
)

type Metric struct {
	Label    `yaml:",inline"`
	DataType string `yaml:"data_type"`
	Unit     string `yaml:"unit"`
}

func (metric Metric) toMetricValueMetadata() (metadata.MetricValueMetadata, error) {
	var valueMetadata metadata.MetricValueMetadata
	var dataType pdata.MetricDataType

	switch metric.DataType {
	case metricDataTypeGauge:
		dataType = pdata.MetricDataTypeGauge
	case metricDataTypeSum:
		dataType = pdata.MetricDataTypeSum
	default:
		return nil, fmt.Errorf("invalid data type received for metric `%v`", metric.Name)
	}

	switch metric.ValueType {
	case metricValueTypeInt:
		valueMetadata = metadata.NewInt64MetricValueMetadata(metric.Name, metric.ColumnName, dataType, metric.Unit)
	case metricValueTypeFloat:
		valueMetadata = metadata.NewFloat64MetricValueMetadata(metric.Name, metric.ColumnName, dataType, metric.Unit)
	default:
		return nil, fmt.Errorf("invalid value type received for metric `%v`", metric.Name)
	}

	return valueMetadata, nil
}
