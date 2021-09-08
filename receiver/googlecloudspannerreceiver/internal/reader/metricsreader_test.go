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

package reader

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	labelName       = "LabelName"
	labelColumnName = "LabelColumnName"

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

func TestMetricsReader_FullName(t *testing.T) {

	metricsReader := MetricsReader{
		Name:         "name",
		ProjectId:    "ProjectId",
		InstanceId:   "InstanceId",
		DatabaseName: "DatabaseName",
	}

	assert.Equal(t, metricsReader.Name+" "+metricsReader.ProjectId+"::"+
		metricsReader.InstanceId+"::"+metricsReader.DatabaseName, (&metricsReader).FullName())
}

func TestMetricsReader_IntervalEnd_TimestampColumnName(t *testing.T) {
	timestamp := time.Now().UTC()
	metricsReader := &MetricsReader{
		TimestampColumnName: timestampColumnName,
	}

	row, _ := spanner.NewRow([]string{timestampColumnName}, []interface{}{timestamp})
	intervalEnd, _ := metricsReader.intervalEnd(row)

	assert.Equal(t, timestamp, intervalEnd)
}

func TestMetricsReader_IntervalEnd_NoTimestampColumnName(t *testing.T) {
	metricsReader := &MetricsReader{}

	row, _ := spanner.NewRow([]string{}, []interface{}{})
	intervalEnd, _ := metricsReader.intervalEnd(row)

	assert.NotNil(t, intervalEnd)
	assert.False(t, intervalEnd.IsZero())
}

func TestMetricsReader_IntervalEnd_Error(t *testing.T) {
	timestamp := time.Now().UTC()
	metricsReader := &MetricsReader{
		TimestampColumnName: "nonExistingColumn",
	}

	row, _ := spanner.NewRow([]string{timestampColumnName}, []interface{}{timestamp})
	_, err := metricsReader.intervalEnd(row)

	require.Error(t, err)
}

func TestToLabelValue_StringLabelValueMetadata(t *testing.T) {
	labelValueMetadata := metadata.StringLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{stringValue})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, metadata.StringLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.GetLabelName())
	assert.Equal(t, labelColumnName, labelValue.GetLabelColumnName())
	assert.Equal(t, stringValue, labelValue.GetValue())
}

func TestToLabelValue_Int64LabelValueMetadata(t *testing.T) {
	labelValueMetadata := metadata.Int64LabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{int64Value})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, metadata.Int64LabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.GetLabelName())
	assert.Equal(t, labelColumnName, labelValue.GetLabelColumnName())
	assert.Equal(t, int64Value, labelValue.GetValue())
}

func TestToLabelValue_BoolLabelValueMetadata(t *testing.T) {
	labelValueMetadata := metadata.BoolLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{boolValue})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, metadata.BoolLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.GetLabelName())
	assert.Equal(t, labelColumnName, labelValue.GetLabelColumnName())
	assert.Equal(t, boolValue, labelValue.GetValue())
}

func TestToLabelValue_StringSliceLabelValueMetadata(t *testing.T) {
	labelValueMetadata := metadata.StringSliceLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{[]string{stringValue, stringValue}})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, metadata.StringSliceLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.GetLabelName())
	assert.Equal(t, labelColumnName, labelValue.GetLabelColumnName())
	assert.Equal(t, stringValue+","+stringValue, labelValue.GetValue())
}

func TestToLabelValue_ByteSliceLabelValueMetadata(t *testing.T) {
	labelValueMetadata := metadata.ByteSliceLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{[]byte(stringValue)})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, metadata.ByteSliceLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.GetLabelName())
	assert.Equal(t, labelColumnName, labelValue.GetLabelColumnName())
	assert.Equal(t, stringValue, labelValue.GetValue())
}

func TestMetricsReader_ToLabelValues_AllPossibleMetadata(t *testing.T) {
	stringLabelValueMetadata := metadata.StringLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "stringLabelName",
			LabelColumnName: "stringLabelColumnName",
		},
	}

	boolLabelValueMetadata := metadata.BoolLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "boolLabelName",
			LabelColumnName: "boolLabelColumnName",
		},
	}

	int64LabelValueMetadata := metadata.Int64LabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "int64LabelName",
			LabelColumnName: "int64LabelColumnName",
		},
	}

	stringSliceLabelValueMetadata := metadata.StringSliceLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "stringSliceLabelName",
			LabelColumnName: "stringSliceLabelColumnName",
		},
	}

	byteSliceLabelValueMetadata := metadata.ByteSliceLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "byteSliceLabelName",
			LabelColumnName: "byteSliceLabelColumnName",
		},
	}

	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		stringLabelValueMetadata,
		boolLabelValueMetadata,
		int64LabelValueMetadata,
		stringSliceLabelValueMetadata,
		byteSliceLabelValueMetadata,
	}

	metricsReader := MetricsReader{
		QueryLabelValuesMetadata: queryLabelValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			stringLabelValueMetadata.LabelColumnName,
			boolLabelValueMetadata.LabelColumnName,
			int64LabelValueMetadata.LabelColumnName,
			stringSliceLabelValueMetadata.LabelColumnName,
			byteSliceLabelValueMetadata.LabelColumnName,
		},
		[]interface{}{
			stringValue,
			boolValue,
			int64Value,
			[]string{stringValue, stringValue},
			[]byte(stringValue),
		})

	labelValues, _ := metricsReader.toLabelValues(row)

	assert.Equal(t, len(queryLabelValuesMetadata), len(labelValues))

	expectedTypes := []metadata.LabelValue{
		metadata.StringLabelValue{},
		metadata.BoolLabelValue{},
		metadata.Int64LabelValue{},
		metadata.StringSliceLabelValue{},
		metadata.ByteSliceLabelValue{},
	}

	for i, expectedType := range expectedTypes {
		assert.IsType(t, expectedType, labelValues[i])
	}
}

func TestMetricsReader_ToLabelValues_Error(t *testing.T) {
	stringLabelValueMetadata := metadata.StringLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "stringLabelName",
			LabelColumnName: "stringLabelColumnName",
		},
	}

	boolLabelValueMetadata := metadata.BoolLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       "nonExisting",
			LabelColumnName: "nonExistingColumn",
		},
	}

	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		stringLabelValueMetadata,
		boolLabelValueMetadata,
	}

	metricsReader := MetricsReader{
		QueryLabelValuesMetadata: queryLabelValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			stringLabelValueMetadata.LabelColumnName},
		[]interface{}{
			stringValue})

	labelValues, err := metricsReader.toLabelValues(row)

	assert.Nil(t, labelValues)
	require.Error(t, err)
}

func TestToMetricValueWithInt64MetricValueMetadata(t *testing.T) {
	metricValueMetadata := metadata.Int64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	row, _ := spanner.NewRow([]string{metricColumnName}, []interface{}{int64Value})
	metricValue, _ := toMetricValue(metricValueMetadata, row)

	assert.IsType(t, metadata.Int64MetricValue{}, metricValue)
	assert.Equal(t, metricName, metricValue.GetMetricName())
	assert.Equal(t, metricColumnName, metricValue.GetMetricColumnName())
	assert.Equal(t, metricDataType, metricValue.GetMetricDataType())
	assert.Equal(t, metricUnit, metricValue.GetMetricUnit())
	assert.Equal(t, int64Value, metricValue.GetValue())
}

func TestToMetricValueWithFloat64MetricValueMetadata(t *testing.T) {
	metricValueMetadata := metadata.Float64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	row, _ := spanner.NewRow([]string{metricColumnName}, []interface{}{float64Value})
	metricValue, _ := toMetricValue(metricValueMetadata, row)

	assert.IsType(t, metadata.Float64MetricValue{}, metricValue)
	assert.Equal(t, metricName, metricValue.GetMetricName())
	assert.Equal(t, metricColumnName, metricValue.GetMetricColumnName())
	assert.Equal(t, metricDataType, metricValue.GetMetricDataType())
	assert.Equal(t, metricUnit, metricValue.GetMetricUnit())
	assert.Equal(t, float64Value, metricValue.GetValue())
}

func TestMetricsReader_ToMetricValues_AllPossibleMetadata(t *testing.T) {
	int64MetricValueMetadata := metadata.Int64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       "int64MetricName",
			MetricColumnName: "int64MetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	float64MetricValueMetadata := metadata.Float64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       "float64MetricName",
			MetricColumnName: "float64MetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		int64MetricValueMetadata,
		float64MetricValueMetadata,
	}

	metricsReader := MetricsReader{
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

	metricValues, _ := metricsReader.toMetricValues(row)

	assert.Equal(t, len(queryMetricValuesMetadata), len(metricValues))

	expectedTypes := []metadata.MetricValue{metadata.Int64MetricValue{}, metadata.Float64MetricValue{}}

	for i, expectedType := range expectedTypes {
		assert.IsType(t, expectedType, metricValues[i])
	}
}

func TestMetricsReader_ToMetricValues_Error(t *testing.T) {
	int64MetricValueMetadata := metadata.Int64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       "int64MetricName",
			MetricColumnName: "int64MetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	float64MetricValueMetadata := metadata.Float64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       "nonExistingMetricName",
			MetricColumnName: "nonExistingMetricColumnName",
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		int64MetricValueMetadata,
		float64MetricValueMetadata,
	}

	metricsReader := MetricsReader{
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			int64MetricValueMetadata.MetricColumnName},
		[]interface{}{
			int64Value,
		})

	metricValues, err := metricsReader.toMetricValues(row)

	assert.Nil(t, metricValues)
	require.Error(t, err)
}

func TestMetricsReader_ToMetrics_MetricDataTypeGauge(t *testing.T) {
	testMetricsReaderToMetricsWithSpecificMetricDataType(t, pdata.MetricDataTypeGauge)
}

func TestMetricsReader_ToMetrics_MetricDataTypeSum(t *testing.T) {
	testMetricsReaderToMetricsWithSpecificMetricDataType(t, pdata.MetricDataTypeSum)
}

func testMetricsReaderToMetricsWithSpecificMetricDataType(t *testing.T, metricDataType pdata.MetricDataType) {
	intervalEnd := time.Now().UTC()

	stringLabelValue := metadata.StringLabelValue{
		StringLabelValueMetadata: metadata.StringLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "stringLabelName",
				LabelColumnName: "stringLabelColumnName",
			},
		},
		Value: stringValue,
	}

	boolLabelValue := metadata.BoolLabelValue{
		BoolLabelValueMetadata: metadata.BoolLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "boolLabelName",
				LabelColumnName: "boolLabelColumnName",
			},
		},
		Value: boolValue,
	}

	int64LabelValue := metadata.Int64LabelValue{
		Int64LabelValueMetadata: metadata.Int64LabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "int64LabelName",
				LabelColumnName: "int64LabelColumnName",
			},
		},
		Value: int64Value,
	}

	stringSliceLabelValue := metadata.StringSliceLabelValue{
		StringSliceLabelValueMetadata: metadata.StringSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "stringSliceLabelName",
				LabelColumnName: "stringSliceLabelColumnName",
			},
		},
		Value: stringValue,
	}

	byteSliceLabelValue := metadata.ByteSliceLabelValue{
		ByteSliceLabelValueMetadata: metadata.ByteSliceLabelValueMetadata{
			QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
				LabelName:       "byteSliceLabelName",
				LabelColumnName: "byteSliceLabelColumnName",
			},
		},
		Value: stringValue,
	}

	labelValues := []metadata.LabelValue{
		stringLabelValue,
		boolLabelValue,
		int64LabelValue,
		stringSliceLabelValue,
		byteSliceLabelValue,
	}

	metricValues := []metadata.MetricValue{
		metadata.Int64MetricValue{
			Int64MetricValueMetadata: metadata.Int64MetricValueMetadata{
				QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
					MetricName:       "int64MetricName",
					MetricColumnName: "int64MetricColumnName",
					MetricDataType:   metricDataType,
					MetricUnit:       metricUnit,
				},
			},
			Value: int64Value,
		},
		metadata.Float64MetricValue{
			Float64MetricValueMetadata: metadata.Float64MetricValueMetadata{
				QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
					MetricName:       "float64MetricName",
					MetricColumnName: "float64MetricColumnName",
					MetricDataType:   metricDataType,
					MetricUnit:       metricUnit,
				},
			},
			Value: float64Value,
		},
	}

	metricsReader := MetricsReader{
		ProjectId:        "ProjectId",
		InstanceId:       "InstanceId",
		DatabaseName:     "DatabaseName",
		MetricNamePrefix: metricNamePrefix,
	}

	metrics := metricsReader.toMetrics(intervalEnd, labelValues, metricValues)

	assert.Equal(t, len(metricValues), len(metrics))

	for i, metric := range metrics {
		assert.Equal(t, 1, metric.DataPointCount())
		assert.Equal(t, 1, metric.MetricCount())
		assert.Equal(t, 1, metric.ResourceMetrics().Len())
		assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().Len())
		assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().Len())

		ilMetric := metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0)

		assert.Equal(t, metricsReader.MetricNamePrefix+metricValues[i].GetMetricName(), ilMetric.Name())
		assert.Equal(t, metricValues[i].GetMetricUnit(), ilMetric.Unit())
		assert.Equal(t, metricValues[i].GetMetricDataType(), ilMetric.DataType())

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
		case metadata.Int64MetricValue:
			assert.Equal(t, metricValues[i].GetValue(), dataPoint.IntVal())
		case metadata.Float64MetricValue:
			assert.Equal(t, metricValues[i].GetValue(), dataPoint.DoubleVal())
		}

		assert.Equal(t, pdata.NewTimestampFromTime(intervalEnd), dataPoint.Timestamp())

		assert.Equal(t, 3+len(labelValues), dataPoint.Attributes().Len())

		attributesMap := dataPoint.Attributes()

		value, exists := attributesMap.Get(projectIdLabelName)

		assert.True(t, exists)
		assert.Equal(t, metricsReader.ProjectId, value.StringVal())

		value, exists = attributesMap.Get(instanceIdLabelName)

		assert.True(t, exists)
		assert.Equal(t, metricsReader.InstanceId, value.StringVal())

		value, exists = attributesMap.Get(databaseLabelName)

		assert.True(t, exists)
		assert.Equal(t, metricsReader.DatabaseName, value.StringVal())

		value, exists = attributesMap.Get(stringLabelValue.GetLabelName())

		assert.True(t, exists)
		assert.Equal(t, stringLabelValue.GetValue(), value.StringVal())

		value, exists = attributesMap.Get(boolLabelValue.GetLabelName())

		assert.True(t, exists)
		assert.Equal(t, boolLabelValue.GetValue(), value.BoolVal())

		value, exists = attributesMap.Get(int64LabelValue.GetLabelName())

		assert.True(t, exists)
		assert.Equal(t, int64LabelValue.GetValue(), value.IntVal())

		value, exists = attributesMap.Get(stringSliceLabelValue.GetLabelName())

		assert.True(t, exists)
		assert.Equal(t, stringSliceLabelValue.GetValue(), value.StringVal())

		value, exists = attributesMap.Get(byteSliceLabelValue.GetLabelName())

		assert.True(t, exists)
		assert.Equal(t, byteSliceLabelValue.GetValue(), value.StringVal())
	}
}

func TestMetricsReader_RowToMetrics(t *testing.T) {
	timestamp := time.Now().UTC()

	labelValueMetadata := metadata.StringLabelValueMetadata{
		QueryLabelValueMetadata: metadata.QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	metricValueMetadata := metadata.Int64MetricValueMetadata{
		QueryMetricValueMetadata: metadata.QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	queryLabelValuesMetadata := []metadata.LabelValueMetadata{labelValueMetadata}
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{metricValueMetadata}

	metricsReader := MetricsReader{
		ProjectId:                 "ProjectId",
		InstanceId:                "InstanceId",
		DatabaseName:              "DatabaseName",
		MetricNamePrefix:          metricNamePrefix,
		TimestampColumnName:       timestampColumnName,
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			labelColumnName,
			metricColumnName,
			timestampColumnName,
		},
		[]interface{}{
			stringValue,
			int64Value,
			timestamp,
		})

	metrics, _ := metricsReader.rowToMetrics(row)

	assert.Equal(t, 1, len(metrics))
}
