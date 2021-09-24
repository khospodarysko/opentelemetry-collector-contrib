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

package metadata

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
)

const (
	projectID    = "ProjectID"
	instanceID   = "InstanceID"
	databaseName = "DatabaseName"
)

func databaseID() *datasource.DatabaseID {
	return datasource.NewDatabaseID(projectID, instanceID, databaseName)
}

func TestMetricsMetadata_Timestamp_TimestampColumnName(t *testing.T) {
	expected := time.Now().UTC()
	metadata := &MetricsMetadata{
		TimestampColumnName: timestampColumnName,
	}

	row, _ := spanner.NewRow([]string{timestampColumnName}, []interface{}{expected})
	timestamp, _ := metadata.timestamp(row)

	assert.Equal(t, expected, timestamp)
}

func TestMetricsMetadata_Timestamp_NoTimestampColumnName(t *testing.T) {
	metadata := &MetricsMetadata{}

	row, _ := spanner.NewRow([]string{}, []interface{}{})
	timestamp, _ := metadata.timestamp(row)

	assert.NotNil(t, timestamp)
	assert.False(t, timestamp.IsZero())
}

func TestMetricsMetadata_Timestamp_Error(t *testing.T) {
	expected := time.Now().UTC()
	metadata := &MetricsMetadata{
		TimestampColumnName: "nonExistingColumn",
	}

	row, _ := spanner.NewRow([]string{timestampColumnName}, []interface{}{expected})
	_, err := metadata.timestamp(row)

	require.Error(t, err)
}

func TestToLabelValue_StringLabelValueMetadata(t *testing.T) {
	labelValueMetadata := StringLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata(labelName, labelColumnName),
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{stringValue})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, stringLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.Name())
	assert.Equal(t, labelColumnName, labelValue.ColumnName())
	assert.Equal(t, stringValue, labelValue.Value())
}

func TestToLabelValue_Int64LabelValueMetadata(t *testing.T) {
	labelValueMetadata := Int64LabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata(labelName, labelColumnName),
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{int64Value})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, int64LabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.Name())
	assert.Equal(t, labelColumnName, labelValue.ColumnName())
	assert.Equal(t, int64Value, labelValue.Value())
}

func TestToLabelValue_BoolLabelValueMetadata(t *testing.T) {
	labelValueMetadata := BoolLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata(labelName, labelColumnName),
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{boolValue})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, boolLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.Name())
	assert.Equal(t, labelColumnName, labelValue.ColumnName())
	assert.Equal(t, boolValue, labelValue.Value())
}

func TestToLabelValue_StringSliceLabelValueMetadata(t *testing.T) {
	labelValueMetadata := StringSliceLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata(labelName, labelColumnName),
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{[]string{stringValue, stringValue}})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, stringSliceLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.Name())
	assert.Equal(t, labelColumnName, labelValue.ColumnName())
	assert.Equal(t, stringValue+","+stringValue, labelValue.Value())
}

func TestToLabelValue_ByteSliceLabelValueMetadata(t *testing.T) {
	labelValueMetadata := ByteSliceLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata(labelName, labelColumnName),
	}

	row, _ := spanner.NewRow([]string{labelColumnName}, []interface{}{[]byte(stringValue)})
	labelValue, _ := toLabelValue(labelValueMetadata, row)

	assert.IsType(t, byteSliceLabelValue{}, labelValue)
	assert.Equal(t, labelName, labelValue.Name())
	assert.Equal(t, labelColumnName, labelValue.ColumnName())
	assert.Equal(t, stringValue, labelValue.Value())
}

func TestMetricsMetadata_ToLabelValues_AllPossibleMetadata(t *testing.T) {
	stringLabelValueMetadata := StringLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("stringLabelName", "stringLabelColumnName"),
	}

	boolLabelValueMetadata := BoolLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("boolLabelName", "boolLabelColumnName"),
	}

	int64LabelValueMetadata := Int64LabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("int64LabelName", "int64LabelColumnName"),
	}

	stringSliceLabelValueMetadata := StringSliceLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("stringSliceLabelName", "stringSliceLabelColumnName"),
	}

	byteSliceLabelValueMetadata := ByteSliceLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("byteSliceLabelName", "byteSliceLabelColumnName"),
	}

	queryLabelValuesMetadata := []LabelValueMetadata{
		stringLabelValueMetadata,
		boolLabelValueMetadata,
		int64LabelValueMetadata,
		stringSliceLabelValueMetadata,
		byteSliceLabelValueMetadata,
	}

	metadata := MetricsMetadata{
		QueryLabelValuesMetadata: queryLabelValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			stringLabelValueMetadata.columnName,
			boolLabelValueMetadata.columnName,
			int64LabelValueMetadata.columnName,
			stringSliceLabelValueMetadata.columnName,
			byteSliceLabelValueMetadata.columnName,
		},
		[]interface{}{
			stringValue,
			boolValue,
			int64Value,
			[]string{stringValue, stringValue},
			[]byte(stringValue),
		})

	labelValues, _ := metadata.toLabelValues(row)

	assert.Equal(t, len(queryLabelValuesMetadata), len(labelValues))

	expectedTypes := []LabelValue{
		stringLabelValue{},
		boolLabelValue{},
		int64LabelValue{},
		stringSliceLabelValue{},
		byteSliceLabelValue{},
	}

	for i, expectedType := range expectedTypes {
		assert.IsType(t, expectedType, labelValues[i])
	}
}

func TestMetricsMetadata_ToLabelValues_Error(t *testing.T) {
	stringLabelValueMetadata := StringLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("stringLabelName", "stringLabelColumnName"),
	}

	boolLabelValueMetadata := BoolLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata("nonExisting", "nonExistingColumn"),
	}

	queryLabelValuesMetadata := []LabelValueMetadata{
		stringLabelValueMetadata,
		boolLabelValueMetadata,
	}

	metadata := MetricsMetadata{
		QueryLabelValuesMetadata: queryLabelValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			stringLabelValueMetadata.columnName},
		[]interface{}{
			stringValue})

	labelValues, err := metadata.toLabelValues(row)

	assert.Nil(t, labelValues)
	require.Error(t, err)
}

func TestToMetricValueWithInt64MetricValueMetadata(t *testing.T) {
	metricValueMetadata := Int64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata(metricName, metricColumnName, metricDataType, metricUnit),
	}

	row, _ := spanner.NewRow([]string{metricColumnName}, []interface{}{int64Value})
	metricValue, _ := toMetricValue(metricValueMetadata, row)

	assert.IsType(t, int64MetricValue{}, metricValue)
	assert.Equal(t, metricName, metricValue.Name())
	assert.Equal(t, metricColumnName, metricValue.ColumnName())
	assert.Equal(t, metricDataType, metricValue.DataType())
	assert.Equal(t, metricUnit, metricValue.Unit())
	assert.Equal(t, int64Value, metricValue.Value())
}

func TestToMetricValueWithFloat64MetricValueMetadata(t *testing.T) {
	metricValueMetadata := Float64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata(metricName, metricColumnName, metricDataType, metricUnit),
	}

	row, _ := spanner.NewRow([]string{metricColumnName}, []interface{}{float64Value})
	metricValue, _ := toMetricValue(metricValueMetadata, row)

	assert.IsType(t, float64MetricValue{}, metricValue)
	assert.Equal(t, metricName, metricValue.Name())
	assert.Equal(t, metricColumnName, metricValue.ColumnName())
	assert.Equal(t, metricDataType, metricValue.DataType())
	assert.Equal(t, metricUnit, metricValue.Unit())
	assert.Equal(t, float64Value, metricValue.Value())
}

func TestMetricsMetadata_ToMetricValues_AllPossibleMetadata(t *testing.T) {
	int64MetricValueMetadata := Int64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata("int64MetricName",
			"int64MetricColumnName", metricDataType, metricUnit),
	}

	float64MetricValueMetadata := Float64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata("float64MetricName",
			"float64MetricColumnName", metricDataType, metricUnit),
	}

	queryMetricValuesMetadata := []MetricValueMetadata{
		int64MetricValueMetadata,
		float64MetricValueMetadata,
	}

	metadata := MetricsMetadata{
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			int64MetricValueMetadata.columnName,
			float64MetricValueMetadata.columnName},
		[]interface{}{
			int64Value,
			float64Value,
		})

	metricValues, _ := metadata.toMetricValues(row)

	assert.Equal(t, len(queryMetricValuesMetadata), len(metricValues))

	expectedTypes := []MetricValue{int64MetricValue{}, float64MetricValue{}}

	for i, expectedType := range expectedTypes {
		assert.IsType(t, expectedType, metricValues[i])
	}
}

func TestMetricsMetadata_ToMetricValues_Error(t *testing.T) {
	int64MetricValueMetadata := Int64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata("int64MetricName",
			"int64MetricColumnName", metricDataType, metricUnit),
	}

	float64MetricValueMetadata := Float64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata("nonExistingMetricName",
			"nonExistingMetricColumnName", metricDataType, metricUnit),
	}

	queryMetricValuesMetadata := []MetricValueMetadata{
		int64MetricValueMetadata,
		float64MetricValueMetadata,
	}

	metadata := MetricsMetadata{
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}

	row, _ := spanner.NewRow(
		[]string{
			int64MetricValueMetadata.columnName},
		[]interface{}{
			int64Value,
		})

	metricValues, err := metadata.toMetricValues(row)

	assert.Nil(t, metricValues)
	require.Error(t, err)
}

func TestMetricsMetadata_ToMetrics_MetricDataTypeGauge(t *testing.T) {
	testMetricsMetadataToMetricsWithSpecificMetricDataType(t, pdata.MetricDataTypeGauge)
}

func TestMetricsMetadata_ToMetrics_MetricDataTypeSum(t *testing.T) {
	testMetricsMetadataToMetricsWithSpecificMetricDataType(t, pdata.MetricDataTypeSum)
}

func testMetricsMetadataToMetricsWithSpecificMetricDataType(t *testing.T, metricDataType pdata.MetricDataType) {
	timestamp := time.Now().UTC()

	stringLabelValue := stringLabelValue{
		StringLabelValueMetadata: StringLabelValueMetadata{
			queryLabelValueMetadata: newQueryLabelValueMetadata("stringLabelName", "stringLabelColumnName"),
		},
		value: stringValue,
	}

	boolLabelValue := boolLabelValue{
		BoolLabelValueMetadata: BoolLabelValueMetadata{
			queryLabelValueMetadata: newQueryLabelValueMetadata("boolLabelName", "boolLabelColumnName"),
		},
		value: boolValue,
	}

	int64LabelValue := int64LabelValue{
		Int64LabelValueMetadata: Int64LabelValueMetadata{
			queryLabelValueMetadata: newQueryLabelValueMetadata("int64LabelName", "int64LabelColumnName"),
		},
		value: int64Value,
	}

	stringSliceLabelValue := stringSliceLabelValue{
		StringSliceLabelValueMetadata: StringSliceLabelValueMetadata{
			queryLabelValueMetadata: newQueryLabelValueMetadata("stringSliceLabelName", "stringSliceLabelColumnName"),
		},
		value: stringValue,
	}

	byteSliceLabelValue := byteSliceLabelValue{
		ByteSliceLabelValueMetadata: ByteSliceLabelValueMetadata{
			queryLabelValueMetadata: newQueryLabelValueMetadata("byteSliceLabelName", "byteSliceLabelColumnName"),
		},
		value: stringValue,
	}

	labelValues := []LabelValue{
		stringLabelValue,
		boolLabelValue,
		int64LabelValue,
		stringSliceLabelValue,
		byteSliceLabelValue,
	}

	metricValues := []MetricValue{
		int64MetricValue{
			Int64MetricValueMetadata: Int64MetricValueMetadata{
				queryMetricValueMetadata: newQueryMetricValueMetadata("int64MetricName",
					"int64MetricColumnName", metricDataType, metricUnit),
			},
			value: int64Value,
		},
		float64MetricValue{
			Float64MetricValueMetadata: Float64MetricValueMetadata{
				queryMetricValueMetadata: newQueryMetricValueMetadata("float64MetricName",
					"float64MetricColumnName", metricDataType, metricUnit),
			},
			value: float64Value,
		},
	}

	databaseID := databaseID()

	metadata := MetricsMetadata{
		MetricNamePrefix: metricNamePrefix,
	}

	metrics := metadata.toMetrics(databaseID, timestamp, labelValues, metricValues)

	assert.Equal(t, len(metricValues), len(metrics))

	for i, metric := range metrics {
		assert.Equal(t, 1, metric.DataPointCount())
		assert.Equal(t, 1, metric.MetricCount())
		assert.Equal(t, 1, metric.ResourceMetrics().Len())
		assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().Len())
		assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().Len())

		ilMetric := metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0)

		assert.Equal(t, metadata.MetricNamePrefix+metricValues[i].Name(), ilMetric.Name())
		assert.Equal(t, metricValues[i].Unit(), ilMetric.Unit())
		assert.Equal(t, metricValues[i].DataType(), ilMetric.DataType())

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
		case int64MetricValue:
			assert.Equal(t, metricValues[i].Value(), dataPoint.IntVal())
		case float64MetricValue:
			assert.Equal(t, metricValues[i].Value(), dataPoint.DoubleVal())
		}

		assert.Equal(t, pdata.NewTimestampFromTime(timestamp), dataPoint.Timestamp())

		assert.Equal(t, 3+len(labelValues), dataPoint.Attributes().Len())

		attributesMap := dataPoint.Attributes()

		value, exists := attributesMap.Get(projectIDLabelName)

		assert.True(t, exists)
		assert.Equal(t, databaseID.ProjectID(), value.StringVal())

		value, exists = attributesMap.Get(instanceIDLabelName)

		assert.True(t, exists)
		assert.Equal(t, databaseID.InstanceID(), value.StringVal())

		value, exists = attributesMap.Get(databaseLabelName)

		assert.True(t, exists)
		assert.Equal(t, databaseID.DatabaseName(), value.StringVal())

		value, exists = attributesMap.Get(stringLabelValue.Name())

		assert.True(t, exists)
		assert.Equal(t, stringLabelValue.Value(), value.StringVal())

		value, exists = attributesMap.Get(boolLabelValue.Name())

		assert.True(t, exists)
		assert.Equal(t, boolLabelValue.Value(), value.BoolVal())

		value, exists = attributesMap.Get(int64LabelValue.Name())

		assert.True(t, exists)
		assert.Equal(t, int64LabelValue.Value(), value.IntVal())

		value, exists = attributesMap.Get(stringSliceLabelValue.Name())

		assert.True(t, exists)
		assert.Equal(t, stringSliceLabelValue.Value(), value.StringVal())

		value, exists = attributesMap.Get(byteSliceLabelValue.Name())

		assert.True(t, exists)
		assert.Equal(t, byteSliceLabelValue.Value(), value.StringVal())
	}
}

func TestMetricsMetadata_RowToMetrics(t *testing.T) {
	timestamp := time.Now().UTC()

	labelValueMetadata := StringLabelValueMetadata{
		queryLabelValueMetadata: newQueryLabelValueMetadata(labelName, labelColumnName),
	}

	metricValueMetadata := Int64MetricValueMetadata{
		queryMetricValueMetadata: newQueryMetricValueMetadata(metricName, metricColumnName, metricDataType, metricUnit),
	}

	queryLabelValuesMetadata := []LabelValueMetadata{labelValueMetadata}
	queryMetricValuesMetadata := []MetricValueMetadata{metricValueMetadata}

	databaseID := databaseID()

	metadata := MetricsMetadata{
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

	metrics, _ := metadata.RowToMetrics(databaseID, row)

	assert.Equal(t, 1, len(metrics))
}

func TestMetricsMetadata_MetadataType(t *testing.T) {
	testCases := map[string]struct {
		timestampColumnName  string
		expectedMetadataType MetricsMetadataType
	}{
		"Current stats metadata":  {"", MetricsMetadataTypeCurrentStats},
		"Interval stats metadata": {"timestampColumnName", MetricsMetadataTypeIntervalStats},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			metricsMetadata := &MetricsMetadata{
				TimestampColumnName: testCase.timestampColumnName,
			}

			assert.Equal(t, testCase.expectedMetadataType, metricsMetadata.MetadataType())
		})
	}
}
