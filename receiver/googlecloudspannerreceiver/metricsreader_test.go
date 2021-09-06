// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlecloudspannerreceiver

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
)

const (
	labelName       = "labelName"
	labelColumnName = "labelColumnName"

	stringValue  = "stringValue"
	int64Value   = int64(64)
	float64Value = float64(64.64)
	boolValue    = true

	metricName       = "metricName"
	metricColumnName = "metricColumnName"
	metricDataType   = pdata.MetricDataTypeGauge
	metricUnit       = "metricUnit"
	metricNamePrefix = "metricNamePrefix-"

	timestampColumnName = "INTERVAL_END"
)

func TestStringLabelValueMetadata(t *testing.T) {
	metadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.getLabelName())
	assert.Equal(t, labelColumnName, metadata.getLabelColumnName())

	var expectedType *string

	assert.IsType(t, expectedType, metadata.valueHolder())
}

func TestInt64LabelValueMetadata(t *testing.T) {
	metadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.getLabelName())
	assert.Equal(t, labelColumnName, metadata.getLabelColumnName())

	var expectedType *int64

	assert.IsType(t, expectedType, metadata.valueHolder())
}

func TestBoolLabelValueMetadata(t *testing.T) {
	metadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.getLabelName())
	assert.Equal(t, labelColumnName, metadata.getLabelColumnName())

	var expectedType *bool

	assert.IsType(t, expectedType, metadata.valueHolder())
}

func TestStringSliceLabelValueMetadata(t *testing.T) {
	metadata := StringSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.getLabelName())
	assert.Equal(t, labelColumnName, metadata.getLabelColumnName())

	var expectedType *[]string

	assert.IsType(t, expectedType, metadata.valueHolder())
}

func TestStringLabelValue(t *testing.T) {
	metadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	labelValue :=
		StringLabelValue{
			StringLabelValueMetadata: metadata,
			value:                    stringValue,
		}

	assert.Equal(t, metadata, labelValue.StringLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.getValue())
}

func TestInt64LabelValue(t *testing.T) {
	metadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	labelValue :=
		Int64LabelValue{
			Int64LabelValueMetadata: metadata,
			value:                   int64Value,
		}

	assert.Equal(t, metadata, labelValue.Int64LabelValueMetadata)
	assert.Equal(t, int64Value, labelValue.getValue())
}

func TestBoolLabelValue(t *testing.T) {
	metadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	labelValue :=
		BoolLabelValue{
			BoolLabelValueMetadata: metadata,
			value:                  boolValue,
		}

	assert.Equal(t, metadata, labelValue.BoolLabelValueMetadata)
	assert.Equal(t, boolValue, labelValue.getValue())
}

func TestStringSliceLabelValue(t *testing.T) {
	metadata := StringSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	labelValue :=
		StringSliceLabelValue{
			StringSliceLabelValueMetadata: metadata,
			value:                         stringValue,
		}

	assert.Equal(t, metadata, labelValue.StringSliceLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.getValue())
}

func TestNewStringLabelValue(t *testing.T) {
	metadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	value := stringValue
	valueHolder := &value

	labelValue := NewStringLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.StringLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.getValue())
}

func TestNewInt64LabelValue(t *testing.T) {
	metadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	value := int64Value
	valueHolder := &value

	labelValue := NewInt64LabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.Int64LabelValueMetadata)
	assert.Equal(t, int64Value, labelValue.getValue())
}

func TestNewBoolLabelValue(t *testing.T) {
	metadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	value := boolValue
	valueHolder := &value

	labelValue := NewBoolLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.BoolLabelValueMetadata)
	assert.Equal(t, boolValue, labelValue.getValue())
}

func TestNewStringSliceLabelValue(t *testing.T) {
	metadata := StringSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	value := []string{"b", "a", "c"}
	expectedValue := "a,b,c"
	valueHolder := &value

	labelValue := NewStringSliceLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.StringSliceLabelValueMetadata)
	assert.Equal(t, expectedValue, labelValue.getValue())
}

func TestInt64MetricValueMetadata(t *testing.T) {
	metadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	assert.Equal(t, metricName, metadata.getMetricName())
	assert.Equal(t, metricColumnName, metadata.getMetricColumnName())
	assert.Equal(t, metricDataType, metadata.getMetricDataType())
	assert.Equal(t, metricUnit, metadata.getMetricUnit())

	var expectedType *int64

	assert.IsType(t, expectedType, metadata.valueHolder())
}

func TestFloat64MetricValueMetadata(t *testing.T) {
	metadata := Float64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	assert.Equal(t, metricName, metadata.getMetricName())
	assert.Equal(t, metricColumnName, metadata.getMetricColumnName())
	assert.Equal(t, metricDataType, metadata.getMetricDataType())
	assert.Equal(t, metricUnit, metadata.getMetricUnit())

	var expectedType *float64

	assert.IsType(t, expectedType, metadata.valueHolder())
}

func TestInt64MetricValue(t *testing.T) {
	metadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	metricValue :=
		Int64MetricValue{
			Int64MetricValueMetadata: metadata,
			value:                    int64Value,
		}

	assert.Equal(t, metadata, metricValue.Int64MetricValueMetadata)
	assert.Equal(t, int64Value, metricValue.getValue())
}

func TestFloat64MetricValue(t *testing.T) {
	metadata := Float64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	metricValue :=
		Float64MetricValue{
			Float64MetricValueMetadata: metadata,
			value:                      float64Value,
		}

	assert.Equal(t, metadata, metricValue.Float64MetricValueMetadata)
	assert.Equal(t, float64Value, metricValue.getValue())
}

func TestNewInt64MetricValue(t *testing.T) {
	metadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	value := int64Value
	valueHolder := &value

	metricValue := NewInt64MetricValue(metadata, valueHolder)

	assert.Equal(t, metadata, metricValue.Int64MetricValueMetadata)
	assert.Equal(t, int64Value, metricValue.getValue())
}

func TestNewFloat64MetricValue(t *testing.T) {
	metadata := Float64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	value := float64Value
	valueHolder := &value

	metricValue := NewFloat64MetricValue(metadata, valueHolder)

	assert.Equal(t, metadata, metricValue.Float64MetricValueMetadata)
	assert.Equal(t, float64Value, metricValue.getValue())
}

func TestMetricsReaderMetadata_IntervalEnd(t *testing.T) {
	timestamp := time.Now().UTC()
	metadata := &MetricsReaderMetadata{
		TimestampColumnName: timestampColumnName,
	}

	row, _ := spanner.NewRow([]string{timestampColumnName}, []interface{}{timestamp})
	intervalEnd, _ := metadata.intervalEnd(row)

	assert.Equal(t, timestamp, intervalEnd)
}

func TestMetricsReaderMetadata_IntervalEnd_Error(t *testing.T) {
	timestamp := time.Now().UTC()
	metadata := &MetricsReaderMetadata{
		TimestampColumnName: "nonExistingColumn",
	}

	row, _ := spanner.NewRow([]string{timestampColumnName}, []interface{}{timestamp})
	_, err := metadata.intervalEnd(row)

	require.Error(t, err)
}

func TestToLabelValue_StringLabelValueMetadata(t *testing.T) {
	labelValueMetadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{stringValue})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, StringLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.getLabelName())
	assert.Equal(t, labelColumnName, labelValue.getLabelColumnName())
	assert.Equal(t, stringValue, labelValue.getValue())
}

func TestToLabelValue_Int64LabelValueMetadata(t *testing.T) {
	labelValueMetadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{int64Value})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, Int64LabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.getLabelName())
	assert.Equal(t, labelColumnName, labelValue.getLabelColumnName())
	assert.Equal(t, int64Value, labelValue.getValue())
}

func TestToLabelValue_BoolLabelValueMetadata(t *testing.T) {
	labelValueMetadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{boolValue})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, BoolLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.getLabelName())
	assert.Equal(t, labelColumnName, labelValue.getLabelColumnName())
	assert.Equal(t, boolValue, labelValue.getValue())
}

func TestMetricsReaderMetadata_ToLabelValues_AllPossibleMetadata(t *testing.T) {
	stringLabelValueMetadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       "stringLabelName",
			labelColumnName: "stringLabelColumnName",
		},
	}

	boolLabelValueMetadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       "boolLabelName",
			labelColumnName: "boolLabelColumnName",
		},
	}

	int64LabelValueMetadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       "int64LabelName",
			labelColumnName: "int64LabelColumnName",
		},
	}

	queryLabelValuesMetadata := []LabelValueMetadata{
		stringLabelValueMetadata,
		boolLabelValueMetadata,
		int64LabelValueMetadata,
	}

	metricsReaderMetadata := MetricsReaderMetadata{
		QueryLabelValuesMetadata: queryLabelValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			stringLabelValueMetadata.labelColumnName,
			boolLabelValueMetadata.labelColumnName,
			int64LabelValueMetadata.labelColumnName},
		[]interface{}{
			stringValue,
			boolValue,
			int64Value})

	labelValues, _ := metricsReaderMetadata.toLabelValues(row)

	assert.Equal(t, len(queryLabelValuesMetadata), len(labelValues))

	expectedTypes := []LabelValue{StringLabelValue{}, BoolLabelValue{}, Int64LabelValue{}}

	for i, expectedType := range expectedTypes {
		assert.IsType(t, expectedType, labelValues[i])
	}
}

func TestMetricsReaderMetadata_ToLabelValues_Error(t *testing.T) {
	stringLabelValueMetadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       "stringLabelName",
			labelColumnName: "stringLabelColumnName",
		},
	}

	boolLabelValueMetadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       "nonExisting",
			labelColumnName: "nonExistingColumn",
		},
	}

	queryLabelValuesMetadata := []LabelValueMetadata{
		stringLabelValueMetadata,
		boolLabelValueMetadata,
	}

	metricsReaderMetadata := MetricsReaderMetadata{
		QueryLabelValuesMetadata: queryLabelValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			stringLabelValueMetadata.labelColumnName},
		[]interface{}{
			stringValue})

	labelValues, err := metricsReaderMetadata.toLabelValues(row)

	assert.Nil(t, labelValues)
	require.Error(t, err)
}

func TestToMetricValueWithInt64MetricValueMetadata(t *testing.T) {
	metricValueMetadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	row, _ := spanner.NewRow([]string{metricColumnName}, []interface{}{int64Value})
	metricValue, _ := toMetricValue(metricValueMetadata, row)

	assert.IsType(t, Int64MetricValue{}, metricValue)
	assert.Equal(t, metricName, metricValue.getMetricName())
	assert.Equal(t, metricColumnName, metricValue.getMetricColumnName())
	assert.Equal(t, metricDataType, metricValue.getMetricDataType())
	assert.Equal(t, metricUnit, metricValue.getMetricUnit())
	assert.Equal(t, int64Value, metricValue.getValue())
}

func TestToMetricValueWithFloat64MetricValueMetadata(t *testing.T) {
	metricValueMetadata := Float64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	row, _ := spanner.NewRow([]string{metricColumnName}, []interface{}{float64Value})
	metricValue, _ := toMetricValue(metricValueMetadata, row)

	assert.IsType(t, Float64MetricValue{}, metricValue)
	assert.Equal(t, metricName, metricValue.getMetricName())
	assert.Equal(t, metricColumnName, metricValue.getMetricColumnName())
	assert.Equal(t, metricDataType, metricValue.getMetricDataType())
	assert.Equal(t, metricUnit, metricValue.getMetricUnit())
	assert.Equal(t, float64Value, metricValue.getValue())
}

func TestMetricsReaderMetadata_ToMetricValues_AllPossibleMetadata(t *testing.T) {
	int64MetricValueMetadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       "int64MetricName",
			MetricColumnName: "int64MetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	float64MetricValueMetadata := Float64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       "float64MetricName",
			MetricColumnName: "float64MetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	queryMetricValuesMetadata := []MetricValueMetadata{
		int64MetricValueMetadata,
		float64MetricValueMetadata,
	}

	metricsReaderMetadata := MetricsReaderMetadata{
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			int64MetricValueMetadata.MetricColumnName,
			float64MetricValueMetadata.MetricColumnName},
		[]interface{}{
			int64Value,
			float64Value,
		})

	metricValues, _ := metricsReaderMetadata.toMetricValues(row)

	assert.Equal(t, len(queryMetricValuesMetadata), len(metricValues))

	expectedTypes := []MetricValue{Int64MetricValue{}, Float64MetricValue{}}

	for i, expectedType := range expectedTypes {
		assert.IsType(t, expectedType, metricValues[i])
	}
}

func TestMetricsReaderMetadata_ToMetricValues_Error(t *testing.T) {
	int64MetricValueMetadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       "int64MetricName",
			MetricColumnName: "int64MetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	float64MetricValueMetadata := Float64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       "nonExistingMetricName",
			MetricColumnName: "nonExistingMetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	queryMetricValuesMetadata := []MetricValueMetadata{
		int64MetricValueMetadata,
		float64MetricValueMetadata,
	}

	metricsReaderMetadata := MetricsReaderMetadata{
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			int64MetricValueMetadata.MetricColumnName},
		[]interface{}{
			int64Value,
		})

	metricValues, err := metricsReaderMetadata.toMetricValues(row)

	assert.Nil(t, metricValues)
	require.Error(t, err)
}

func TestMetricsReaderMetadata_ToMetrics_MetricDataTypeGauge(t *testing.T) {
	testMetricsReaderMetadataToMetricsWithSpecificMetricDataType(t, pdata.MetricDataTypeGauge)
}

func TestMetricsReaderMetadata_ToMetrics_MetricDataTypeSum(t *testing.T) {
	testMetricsReaderMetadataToMetricsWithSpecificMetricDataType(t, pdata.MetricDataTypeSum)
}

func testMetricsReaderMetadataToMetricsWithSpecificMetricDataType(t *testing.T, metricDataType pdata.MetricDataType) {
	intervalEnd := time.Now().UTC()

	stringLabelValue := StringLabelValue{
		StringLabelValueMetadata: StringLabelValueMetadata{
			QueryLabelValueMetadata{
				labelName:       "stringLabelName",
				labelColumnName: "stringLabelColumnName",
			},
		},
		value: stringValue,
	}

	boolLabelValue := BoolLabelValue{
		BoolLabelValueMetadata: BoolLabelValueMetadata{
			QueryLabelValueMetadata{
				labelName:       "boolLabelName",
				labelColumnName: "boolLabelColumnName",
			},
		},
		value: boolValue,
	}

	int64LabelValue := Int64LabelValue{
		Int64LabelValueMetadata: Int64LabelValueMetadata{
			QueryLabelValueMetadata{
				labelName:       "int64LabelName",
				labelColumnName: "int64LabelColumnName",
			},
		},
		value: int64Value,
	}

	labelValues := []LabelValue{
		stringLabelValue,
		boolLabelValue,
		int64LabelValue,
	}

	metricValues := []MetricValue{
		Int64MetricValue{
			Int64MetricValueMetadata: Int64MetricValueMetadata{
				QueryMetricValueMetadata{
					MetricName:       "int64MetricName",
					MetricColumnName: "int64MetricColumnName",
					MetricDataType:   metricDataType,
					MetricUnit:       metricUnit,
				},
			},
			value: int64Value,
		},
		Float64MetricValue{
			Float64MetricValueMetadata: Float64MetricValueMetadata{
				QueryMetricValueMetadata{
					MetricName:       "float64MetricName",
					MetricColumnName: "float64MetricColumnName",
					MetricDataType:   metricDataType,
					MetricUnit:       metricUnit,
				},
			},
			value: float64Value,
		},
	}

	metricsReaderMetadata := MetricsReaderMetadata{
		projectId:        "projectId",
		instanceId:       "instanceId",
		databaseName:     "databaseName",
		MetricNamePrefix: metricNamePrefix,
	}

	metrics := metricsReaderMetadata.toMetrics(intervalEnd, labelValues, metricValues)

	assert.Equal(t, len(metricValues), len(metrics))

	for i, metric := range metrics {
		assert.Equal(t, 1, metric.DataPointCount())
		assert.Equal(t, 1, metric.MetricCount())
		assert.Equal(t, 1, metric.ResourceMetrics().Len())
		assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().Len())
		assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().Len())

		ilMetric := metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0)

		assert.Equal(t, metricsReaderMetadata.MetricNamePrefix+metricValues[i].getMetricName(), ilMetric.Name())
		assert.Equal(t, metricValues[i].getMetricUnit(), ilMetric.Unit())
		assert.Equal(t, metricValues[i].getMetricDataType(), ilMetric.DataType())

		var dataPoint pdata.NumberDataPoint

		if metricDataType == pdata.MetricDataTypeGauge {
			assert.NotNil(t, ilMetric.Gauge())
			assert.Equal(t, 1, ilMetric.Gauge().DataPoints().Len())
			dataPoint = ilMetric.Gauge().DataPoints().At(0)
		} else {
			assert.NotNil(t, ilMetric.Sum())
			assert.Equal(t, 1, ilMetric.Sum().DataPoints().Len())
			dataPoint = ilMetric.Sum().DataPoints().At(0)
		}

		switch metricValues[i].(type) {
		case Int64MetricValue:
			assert.Equal(t, metricValues[i].getValue(), dataPoint.IntVal())
		case Float64MetricValue:
			assert.Equal(t, metricValues[i].getValue(), dataPoint.DoubleVal())
		}

		assert.Equal(t, pdata.NewTimestampFromTime(intervalEnd), dataPoint.Timestamp())

		assert.Equal(t, 3+len(labelValues), dataPoint.Attributes().Len())

		attributesMap := dataPoint.Attributes()

		value, exists := attributesMap.Get(projectIdLabelName)

		assert.True(t, exists)
		assert.Equal(t, metricsReaderMetadata.projectId, value.StringVal())

		value, exists = attributesMap.Get(instanceIdLabelName)

		assert.True(t, exists)
		assert.Equal(t, metricsReaderMetadata.instanceId, value.StringVal())

		value, exists = attributesMap.Get(databaseLabelName)

		assert.True(t, exists)
		assert.Equal(t, metricsReaderMetadata.databaseName, value.StringVal())

		value, exists = attributesMap.Get(stringLabelValue.getLabelName())

		assert.True(t, exists)
		assert.Equal(t, stringLabelValue.getValue(), value.StringVal())

		value, exists = attributesMap.Get(boolLabelValue.getLabelName())

		assert.True(t, exists)
		assert.Equal(t, boolLabelValue.getValue(), value.BoolVal())

		value, exists = attributesMap.Get(int64LabelValue.getLabelName())

		assert.True(t, exists)
		assert.Equal(t, int64LabelValue.getValue(), value.IntVal())
	}
}

func TestMetricsReaderMetadata_rowToMetrics(t *testing.T) {
	timestamp := time.Now().UTC()

	labelValueMetadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			labelName:       labelName,
			labelColumnName: labelColumnName,
		},
	}

	metricValueMetadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	queryLabelValuesMetadata := []LabelValueMetadata{labelValueMetadata}
	queryMetricValuesMetadata := []MetricValueMetadata{metricValueMetadata}

	metricsReaderMetadata := MetricsReaderMetadata{
		projectId:                 "projectId",
		instanceId:                "instanceId",
		databaseName:              "databaseName",
		MetricNamePrefix:          metricNamePrefix,
		TimestampColumnName:       timestampColumnName,
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow([]string{labelColumnName, metricColumnName, timestampColumnName}, []interface{}{stringValue, int64Value, timestamp})

	metrics, _ := metricsReaderMetadata.rowToMetrics(row)

	assert.Equal(t, 1, len(metrics))
}
