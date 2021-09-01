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

	"github.com/stretchr/testify/assert"
)

func TestInt64MetricValueMetadata(t *testing.T) {
	metadata := Int64MetricValueMetadata{
		QueryMetricValueMetadata{
			MetricName:       metricName,
			MetricColumnName: metricColumnName,
			MetricDataType:   metricDataType,
			MetricUnit:       metricUnit,
		},
	}

	assert.Equal(t, metricName, metadata.Name())
	assert.Equal(t, metricColumnName, metadata.ColumnName())
	assert.Equal(t, metricDataType, metadata.DataType())
	assert.Equal(t, metricUnit, metadata.Unit())

	var expectedType *int64

	assert.IsType(t, expectedType, metadata.ValueHolder())
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

	assert.Equal(t, metricName, metadata.Name())
	assert.Equal(t, metricColumnName, metadata.ColumnName())
	assert.Equal(t, metricDataType, metadata.DataType())
	assert.Equal(t, metricUnit, metadata.Unit())

	var expectedType *float64

	assert.IsType(t, expectedType, metadata.ValueHolder())
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
			Val:                      int64Value,
		}

	assert.Equal(t, metadata, metricValue.Int64MetricValueMetadata)
	assert.Equal(t, int64Value, metricValue.Value())
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
			Val:                        float64Value,
		}

	assert.Equal(t, metadata, metricValue.Float64MetricValueMetadata)
	assert.Equal(t, float64Value, metricValue.Value())
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
	assert.Equal(t, int64Value, metricValue.Value())
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
	assert.Equal(t, float64Value, metricValue.Value())
}
