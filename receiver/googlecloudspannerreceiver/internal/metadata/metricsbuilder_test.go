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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/filter"
)

const (
	metricName1 = "metricName1"
	metricName2 = "metricName2"
)

type errorItemFilterResolver struct {
}

func (r errorItemFilterResolver) Resolve(string) (filter.ItemFilter, error) {
	return nil, errors.New("error on resolve")
}

func (r errorItemFilterResolver) Shutdown() error {
	return errors.New("error on shutdown")
}

type errorFilter struct {
}

func (f errorFilter) Filter(source []*filter.Item) ([]*filter.Item, error) {
	return nil, errors.New("error on filter")
}

func (f errorFilter) Shutdown() error {
	return nil
}

func (f errorFilter) TotalLimit() int {
	return 0
}

func (f errorFilter) LimitByTimestamp() int {
	return 0
}

type itemFilterResolverWithErrorFilter struct {
}

func (r itemFilterResolverWithErrorFilter) Resolve(string) (filter.ItemFilter, error) {
	return errorFilter{}, nil
}

func (r itemFilterResolverWithErrorFilter) Shutdown() error {
	return nil
}

type testData struct {
	dataPoints           []*MetricsDataPoint
	expectedGroupingKeys []MetricsDataPointKey
	expectedGroups       map[MetricsDataPointKey][]*MetricsDataPoint
}

func TestNewMetricsFromDataPointBuilder(t *testing.T) {
	itemFilterResolver := filter.NewNopItemFilterResolver()

	builder := NewMetricsFromDataPointBuilder(itemFilterResolver)
	builderCasted := builder.(*metricsFromDataPointBuilder)
	defer executeShutdown(t, builderCasted, false)

	assert.Equal(t, itemFilterResolver, builderCasted.filterResolver)
}

func TestMetricsFromDataPointBuilder_Build(t *testing.T) {
	testCases := map[string]struct {
		metricsDataType    pdata.MetricDataType
		itemFilterResolver filter.ItemFilterResolver
		expectError        bool
	}{
		"Gauge":                      {pdata.MetricDataTypeGauge, filter.NewNopItemFilterResolver(), false},
		"Sum":                        {pdata.MetricDataTypeSum, filter.NewNopItemFilterResolver(), false},
		"Gauge with filtering error": {pdata.MetricDataTypeGauge, errorItemFilterResolver{}, true},
		"Sum with filtering error":   {pdata.MetricDataTypeSum, errorItemFilterResolver{}, true},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			testMetricsFromDataPointBuilderBuild(t, testCase.metricsDataType, testCase.itemFilterResolver, testCase.expectError)
		})
	}
}

func testMetricsFromDataPointBuilderBuild(t *testing.T, metricDataType pdata.MetricDataType,
	itemFilterResolver filter.ItemFilterResolver, expectError bool) {

	dataForTesting := generateTestData(metricDataType)
	builder := &metricsFromDataPointBuilder{filterResolver: itemFilterResolver}
	defer executeShutdown(t, builder, expectError)
	expectedGroupingKeysByMetricName := make(map[string]MetricsDataPointKey, len(dataForTesting.expectedGroupingKeys))

	for _, expectedGroupingKey := range dataForTesting.expectedGroupingKeys {
		expectedGroupingKeysByMetricName[expectedGroupingKey.MetricName] = expectedGroupingKey
	}

	metric, err := builder.Build(dataForTesting.dataPoints)
	if expectError {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)

	assert.Equal(t, len(dataForTesting.dataPoints), metric.DataPointCount())
	assert.Equal(t, len(dataForTesting.expectedGroups), metric.MetricCount())
	assert.Equal(t, 1, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().Len())
	assert.Equal(t, len(dataForTesting.expectedGroups), metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().Len())
	require.Equal(t, instrumentationLibraryName, metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).InstrumentationLibrary().Name())

	for i := 0; i < len(dataForTesting.expectedGroups); i++ {
		ilMetric := metric.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(i)
		expectedGroupingKey := expectedGroupingKeysByMetricName[ilMetric.Name()]
		expectedDataPoints := dataForTesting.expectedGroups[expectedGroupingKey]

		for dataPointIndex, expectedDataPoint := range expectedDataPoints {
			assert.Equal(t, expectedDataPoint.metricName, ilMetric.Name())
			assert.Equal(t, expectedDataPoint.metricValue.Unit(), ilMetric.Unit())
			assert.Equal(t, expectedDataPoint.metricValue.DataType().MetricDataType(), ilMetric.DataType())

			var dataPoint pdata.NumberDataPoint

			if metricDataType == pdata.MetricDataTypeGauge {
				assert.NotNil(t, ilMetric.Gauge())
				assert.Equal(t, len(expectedDataPoints), ilMetric.Gauge().DataPoints().Len())
				dataPoint = ilMetric.Gauge().DataPoints().At(dataPointIndex)
			} else {
				assert.NotNil(t, ilMetric.Sum())
				assert.Equal(t, pdata.MetricAggregationTemporalityDelta, ilMetric.Sum().AggregationTemporality())
				assert.True(t, ilMetric.Sum().IsMonotonic())
				assert.Equal(t, len(expectedDataPoints), ilMetric.Sum().DataPoints().Len())
				dataPoint = ilMetric.Sum().DataPoints().At(dataPointIndex)
			}

			assertMetricValue(t, expectedDataPoint.metricValue, dataPoint)

			assert.Equal(t, pdata.NewTimestampFromTime(expectedDataPoint.timestamp), dataPoint.Timestamp())
			// Adding +3 here because we'll always have 3 labels added for each metric: project_id, instance_id, database
			assert.Equal(t, 3+len(expectedDataPoint.labelValues), dataPoint.Attributes().Len())

			attributesMap := dataPoint.Attributes()

			assertDefaultLabels(t, attributesMap, expectedDataPoint.databaseID)
			assertNonDefaultLabels(t, attributesMap, expectedDataPoint.labelValues)
		}
	}
}

func TestMetricsFromDataPointBuilder_GroupAndFilter(t *testing.T) {
	testCases := map[string]struct {
		itemFilterResolver filter.ItemFilterResolver
		expectError        bool
	}{
		"Happy path":           {filter.NewNopItemFilterResolver(), false},
		"With filtering error": {errorItemFilterResolver{}, true},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			builder := &metricsFromDataPointBuilder{
				filterResolver: testCase.itemFilterResolver,
			}
			defer executeShutdown(t, builder, testCase.expectError)
			dataForTesting := generateTestData(metricDataType)

			groupedDataPoints, err := builder.groupAndFilter(dataForTesting.dataPoints)

			if testCase.expectError {
				require.Error(t, err)
				require.Nil(t, groupedDataPoints)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, groupedDataPoints)

			assert.Equal(t, len(dataForTesting.expectedGroups), len(groupedDataPoints))

			for expectedGroupingKey, expectedGroupPoints := range dataForTesting.expectedGroups {
				dataPointsByKey := groupedDataPoints[expectedGroupingKey]

				assert.Equal(t, len(expectedGroupPoints), len(dataPointsByKey))

				for i, point := range expectedGroupPoints {
					assert.Equal(t, point, dataPointsByKey[i])
				}
			}
		})
	}
}

func TestMetricsFromDataPointBuilder_GroupAndFilter_NilDataPoints(t *testing.T) {
	builder := &metricsFromDataPointBuilder{
		filterResolver: filter.NewNopItemFilterResolver(),
	}
	defer executeShutdown(t, builder, false)

	groupedDataPoints, err := builder.groupAndFilter(nil)

	require.NoError(t, err)

	assert.Equal(t, 0, len(groupedDataPoints))
}

func TestMetricsFromDataPointBuilder_Filter(t *testing.T) {
	dataForTesting := generateTestData(metricDataType)
	testCases := map[string]struct {
		itemFilterResolver filter.ItemFilterResolver
		expectError        bool
	}{
		"Happy path":      {filter.NewNopItemFilterResolver(), false},
		"Error on filter": {itemFilterResolverWithErrorFilter{}, true},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			builder := &metricsFromDataPointBuilder{
				filterResolver: testCase.itemFilterResolver,
			}
			defer executeShutdown(t, builder, false)

			filteredDataPoints, err := builder.filter(metricName1, dataForTesting.dataPoints)
			if testCase.expectError {
				require.Error(t, err)
				require.Nil(t, filteredDataPoints)
			} else {
				require.NoError(t, err)
				assert.Equal(t, dataForTesting.dataPoints, filteredDataPoints)
			}
		})
	}
}

func TestMetricsFromDataPointBuilder_Shutdown(t *testing.T) {
	testCases := map[string]struct {
		itemFilterResolver filter.ItemFilterResolver
		expectError        bool
	}{
		"Happy path": {filter.NewNopItemFilterResolver(), false},
		"Error":      {errorItemFilterResolver{}, true},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			builder := &metricsFromDataPointBuilder{
				filterResolver: testCase.itemFilterResolver,
			}

			executeShutdown(t, builder, testCase.expectError)
		})
	}
}

func generateTestData(metricDataType pdata.MetricDataType) testData {
	timestamp1 := time.Now().UTC()
	timestamp2 := timestamp1.Add(time.Minute)
	labelValues := allPossibleLabelValues()
	metricValues := allPossibleMetricValues(metricDataType)

	dataPoints := []*MetricsDataPoint{
		newMetricDataPoint(metricName1, timestamp1, labelValues, metricValues[0]),
		newMetricDataPoint(metricName1, timestamp1, labelValues, metricValues[1]),
		newMetricDataPoint(metricName2, timestamp1, labelValues, metricValues[0]),
		newMetricDataPoint(metricName2, timestamp1, labelValues, metricValues[1]),
		newMetricDataPoint(metricName1, timestamp2, labelValues, metricValues[0]),
		newMetricDataPoint(metricName1, timestamp2, labelValues, metricValues[1]),
		newMetricDataPoint(metricName2, timestamp2, labelValues, metricValues[0]),
		newMetricDataPoint(metricName2, timestamp2, labelValues, metricValues[1]),
	}

	expectedGroupingKeys := []MetricsDataPointKey{
		{
			MetricName:     metricName1,
			MetricDataType: metricValues[0].DataType(),
			MetricUnit:     metricValues[0].Unit(),
		},
		{
			MetricName:     metricName2,
			MetricDataType: metricValues[0].DataType(),
			MetricUnit:     metricValues[0].Unit(),
		},
	}

	expectedGroups := map[MetricsDataPointKey][]*MetricsDataPoint{
		expectedGroupingKeys[0]: {
			dataPoints[0], dataPoints[1], dataPoints[4], dataPoints[5],
		},
		expectedGroupingKeys[1]: {
			dataPoints[2], dataPoints[3], dataPoints[6], dataPoints[7],
		},
	}

	return testData{dataPoints: dataPoints, expectedGroupingKeys: expectedGroupingKeys, expectedGroups: expectedGroups}
}

func newMetricDataPoint(metricName string, timestamp time.Time, labelValues []LabelValue, metricValue MetricValue) *MetricsDataPoint {
	return &MetricsDataPoint{
		metricName:  metricName,
		timestamp:   timestamp,
		databaseID:  databaseID(),
		labelValues: labelValues,
		metricValue: metricValue,
	}
}

func executeShutdown(t *testing.T, metricsBuilder MetricsBuilder, expectError bool) {
	err := metricsBuilder.Shutdown()
	if expectError {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
}
