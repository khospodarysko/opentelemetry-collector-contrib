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

package metadata

import "go.opentelemetry.io/collector/model/pdata"

type MetricValueMetadata interface {
	ValueMetadata
	DataType() pdata.MetricDataType
	Unit() string
}

type MetricValue interface {
	MetricValueMetadata
	Value() interface{}
}

type QueryMetricValueMetadata struct {
	MetricName       string
	MetricColumnName string
	MetricDataType   pdata.MetricDataType
	MetricUnit       string
}

type Int64MetricValueMetadata struct {
	QueryMetricValueMetadata
}

type Float64MetricValueMetadata struct {
	QueryMetricValueMetadata
}

type Int64MetricValue struct {
	Int64MetricValueMetadata
	Val int64
}

type Float64MetricValue struct {
	Float64MetricValueMetadata
	Val float64
}

func (metadata QueryMetricValueMetadata) Name() string {
	return metadata.MetricName
}

func (metadata QueryMetricValueMetadata) ColumnName() string {
	return metadata.MetricColumnName
}

func (metadata QueryMetricValueMetadata) DataType() pdata.MetricDataType {
	return metadata.MetricDataType
}

func (metadata QueryMetricValueMetadata) Unit() string {
	return metadata.MetricUnit
}

func (metadata Int64MetricValueMetadata) ValueHolder() interface{} {
	var valueHolder int64

	return &valueHolder
}

func (metadata Float64MetricValueMetadata) ValueHolder() interface{} {
	var valueHolder float64

	return &valueHolder
}

func (value Int64MetricValue) Value() interface{} {
	return value.Val
}

func (value Float64MetricValue) Value() interface{} {
	return value.Val
}

func NewInt64MetricValue(metadata Int64MetricValueMetadata, valueHolder interface{}) Int64MetricValue {
	return Int64MetricValue{
		Int64MetricValueMetadata: metadata,
		Val:                      *valueHolder.(*int64),
	}
}

func NewFloat64MetricValue(metadata Float64MetricValueMetadata, valueHolder interface{}) Float64MetricValue {
	return Float64MetricValue{
		Float64MetricValueMetadata: metadata,
		Val:                        *valueHolder.(*float64),
	}
}
