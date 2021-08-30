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

package googlecloudspannerreceiver

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"time"
)

type LabelValueMetadata interface {
	getLabelName() string
	getLabelColumnName() string
	valueHolder() interface{}
}

type LabelValue interface {
	LabelValueMetadata
	getValue() interface{}
}

type QueryLabelValueMetadata struct {
	labelName       string
	labelColumnName string
}

type StringLabelValueMetadata struct {
	QueryLabelValueMetadata
}

type Int64LabelValueMetadata struct {
	QueryLabelValueMetadata
}

type BoolLabelValueMetadata struct {
	QueryLabelValueMetadata
}

type StringLabelValue struct {
	StringLabelValueMetadata
	value string
}

type Int64LabelValue struct {
	Int64LabelValueMetadata
	value int64
}

type BoolLabelValue struct {
	BoolLabelValueMetadata
	value bool
}

func (metadata QueryLabelValueMetadata) getLabelName() string {
	return metadata.labelName
}

func (metadata QueryLabelValueMetadata) getLabelColumnName() string {
	return metadata.labelColumnName
}

func (metadata StringLabelValueMetadata) valueHolder() interface{} {
	var valueHolder string

	return &valueHolder
}

func (value StringLabelValue) getValue() interface{} {
	return value.value
}

func NewStringLabelValue(metadata StringLabelValueMetadata, valueHolder interface{}) StringLabelValue {
	return StringLabelValue{
		StringLabelValueMetadata: metadata,
		value:                    *valueHolder.(*string),
	}
}

func (metadata Int64LabelValueMetadata) valueHolder() interface{} {
	var valueHolder int64

	return &valueHolder
}

func (value Int64LabelValue) getValue() interface{} {
	return value.value
}

func NewInt64LabelValue(metadata Int64LabelValueMetadata, valueHolder interface{}) Int64LabelValue {
	return Int64LabelValue{
		Int64LabelValueMetadata: metadata,
		value:                   *valueHolder.(*int64),
	}
}

func (metadata BoolLabelValueMetadata) valueHolder() interface{} {
	var valueHolder bool

	return &valueHolder
}

func (value BoolLabelValue) getValue() interface{} {
	return value.value
}

func NewBoolLabelValue(metadata BoolLabelValueMetadata, valueHolder interface{}) BoolLabelValue {
	return BoolLabelValue{
		BoolLabelValueMetadata: metadata,
		value:                  *valueHolder.(*bool),
	}
}

/* ------------------------------------------------------------------------------------------------------------------ */

type MetricValueMetadata interface {
	getMetricName() string
	getMetricColumnName() string
	getMetricDataType() pdata.MetricDataType
	getMetricUnit() string
	valueHolder() interface{}
}

type MetricValue interface {
	MetricValueMetadata
	getValue() interface{}
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
	value int64
}

type Float64MetricValue struct {
	Float64MetricValueMetadata
	value float64
}

func (metadata QueryMetricValueMetadata) getMetricName() string {
	return metadata.MetricName
}

func (metadata QueryMetricValueMetadata) getMetricColumnName() string {
	return metadata.MetricColumnName
}

func (metadata QueryMetricValueMetadata) getMetricDataType() pdata.MetricDataType {
	return metadata.MetricDataType
}

func (metadata QueryMetricValueMetadata) getMetricUnit() string {
	return metadata.MetricUnit
}

func (metadata Int64MetricValueMetadata) valueHolder() interface{} {
	var valueHolder int64

	return &valueHolder
}

func (metadata Float64MetricValueMetadata) valueHolder() interface{} {
	var valueHolder float64

	return &valueHolder
}

func (value Int64MetricValue) getValue() interface{} {
	return value.value
}

func (value Float64MetricValue) getValue() interface{} {
	return value.value
}

func NewInt64MetricValue(metadata Int64MetricValueMetadata, valueHolder interface{}) Int64MetricValue {
	return Int64MetricValue{
		Int64MetricValueMetadata: metadata,
		value:                    *valueHolder.(*int64),
	}
}

func NewFloat64MetricValue(metadata Float64MetricValueMetadata, valueHolder interface{}) Float64MetricValue {
	return Float64MetricValue{
		Float64MetricValueMetadata: metadata,
		value:                      *valueHolder.(*float64),
	}
}

type MetricsReaderMetadata struct {
	Name                string
	projectId           string
	instanceId          string
	databaseName        string
	Query               string
	TimestampColumnName string
	// In addition to common metric labels
	QueryLabelValuesMetadata  []LabelValueMetadata
	QueryMetricValuesMetadata []MetricValueMetadata
}

/* ------------------------------------------------------------------------------------------------------------------ */

func (metadata *MetricsReaderMetadata) ReadMetrics(ctx context.Context, client *spanner.Client, logger *zap.Logger) ([]pdata.Metrics, error) {
	logger.Info(fmt.Sprintf("Executing read method for metrics metadata %v", metadata.Name))

	stmt := spanner.Statement{SQL: metadata.Query}
	rowsIterator := client.Single().Query(ctx, stmt)
	defer rowsIterator.Stop()

	var collectedMetrics []pdata.Metrics

	for {
		row, err := rowsIterator.Next()

		if err == iterator.Done {
			break
		} else if err != nil {
			logger.Info(fmt.Sprintf("Query %v failed with %v", metadata.Query, err))
			return nil, err
		}

		intervalEnd, err := metadata.intervalEnd(row)

		if err != nil {
			logger.Error(fmt.Sprintf("Error occurred during extracting interval end %v", err))
			return nil, err
		}

		logger.Info(fmt.Sprintf("Interval end = %v", intervalEnd))

		// Reading labels
		labelValues, err := metadata.toLabelValues(row)

		// Reading metrics
		metricValues, err := metadata.toMetricValues(row)

		collectedMetrics = append(collectedMetrics, metadata.toMetrics(intervalEnd, labelValues, metricValues)...)
	}

	return collectedMetrics, nil
}

func (metadata *MetricsReaderMetadata) intervalEnd(row *spanner.Row) (time.Time, error) {
	var intervalEnd time.Time

	err := row.ColumnByName(metadata.TimestampColumnName, &intervalEnd)

	return intervalEnd, err
}

func (metadata *MetricsReaderMetadata) toLabelValues(row *spanner.Row) ([]LabelValue, error) {
	values := make([]LabelValue, len(metadata.QueryLabelValuesMetadata))

	for i, metadataItems := range metadata.QueryLabelValuesMetadata {
		var err error

		if values[i], err = toLabelValue(metadataItems, row); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func toLabelValue(metadata LabelValueMetadata, row *spanner.Row) (LabelValue, error) {
	valueHolder := metadata.valueHolder()

	err := row.ColumnByName(metadata.getLabelColumnName(), valueHolder)

	var value LabelValue = nil

	switch metadataCasted := metadata.(type) {
	case StringLabelValueMetadata:
		value = NewStringLabelValue(metadataCasted, valueHolder)
	case Int64LabelValueMetadata:
		value = NewInt64LabelValue(metadataCasted, valueHolder)
	case BoolLabelValueMetadata:
		value = NewBoolLabelValue(metadataCasted, valueHolder)
	}

	return value, err
}

func (metadata *MetricsReaderMetadata) toMetricValues(row *spanner.Row) ([]MetricValue, error) {
	values := make([]MetricValue, len(metadata.QueryMetricValuesMetadata))

	for i, metadataItems := range metadata.QueryMetricValuesMetadata {
		var err error

		if values[i], err = toMetricValue(metadataItems, row); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func toMetricValue(metadata MetricValueMetadata, row *spanner.Row) (MetricValue, error) {
	valueHolder := metadata.valueHolder()

	err := row.ColumnByName(metadata.getMetricColumnName(), valueHolder)

	var value MetricValue = nil

	switch metadataCasted := metadata.(type) {
	case Int64MetricValueMetadata:
		value = NewInt64MetricValue(metadataCasted, valueHolder)
	case Float64MetricValueMetadata:
		value = NewFloat64MetricValue(metadataCasted, valueHolder)
	}

	return value, err
}

func (metadata *MetricsReaderMetadata) toMetrics(intervalEnd time.Time, labelValues []LabelValue, metricValues []MetricValue) []pdata.Metrics {
	metrics := make([]pdata.Metrics, len(metricValues))

	for i, metricValue := range metricValues {
		md := pdata.NewMetrics()
		rms := md.ResourceMetrics()
		rm := rms.AppendEmpty()

		ilms := rm.InstrumentationLibraryMetrics()
		ilm := ilms.AppendEmpty()
		metric := ilm.Metrics().AppendEmpty()
		metric.SetName(metricValue.getMetricName())
		metric.SetUnit(metricValue.getMetricUnit())
		metric.SetDataType(metricValue.getMetricDataType())
		gauge := metric.Gauge()
		dataPoints := gauge.DataPoints()
		dataPoint := dataPoints.AppendEmpty()

		if valueCasted, ok := metricValue.(Float64MetricValue); ok {
			dataPoint.SetDoubleVal(valueCasted.value)
		} else if valueCasted, ok := metricValue.(Int64MetricValue); ok {
			dataPoint.SetIntVal(valueCasted.value)
		}

		dataPoint.SetTimestamp(pdata.NewTimestampFromTime(intervalEnd))

		for _, labelValue := range labelValues {
			if valueCasted, ok := labelValue.(StringLabelValue); ok {
				dataPoint.Attributes().InsertString(valueCasted.labelName, valueCasted.value)
			} else if valueCasted, ok := labelValue.(BoolLabelValue); ok {
				dataPoint.Attributes().InsertBool(valueCasted.labelName, valueCasted.value)
			}
		}

		dataPoint.Attributes().InsertString("project_id", metadata.projectId)
		dataPoint.Attributes().InsertString("instance_id", metadata.instanceId)
		dataPoint.Attributes().InsertString("database", metadata.databaseName)

		metrics[i] = md
	}

	return metrics
}

/* ------------------------------------------------------------------------------------------------------------------ */

func NewTopQueryStatsMetricsReaderMetadata(
	projectId string,
	instanceId string,
	databaseName string) *MetricsReaderMetadata {

	query := "SELECT * FROM spanner_sys.query_stats_top_minute " +
		"WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.query_stats_top_minute)" +
		"ORDER BY AVG_CPU_SECONDS DESC LIMIT 10"
	timestampColumnName := "INTERVAL_END"

	// Labels
	queryLabelValuesMetadata := []LabelValueMetadata{
		StringLabelValueMetadata{
			QueryLabelValueMetadata{
				labelName:       "query_text",
				labelColumnName: "TEXT",
			},
		},

		BoolLabelValueMetadata{
			QueryLabelValueMetadata{
				labelName:       "query_text_truncated",
				labelColumnName: "TEXT_TRUNCATED",
			},
		},

		Int64LabelValueMetadata{
			QueryLabelValueMetadata{
				labelName:       "query_text_fingerprint",
				labelColumnName: "TEXT_FINGERPRINT",
			},
		},
	}

	// Metrics
	queryMetricValuesMetadata := []MetricValueMetadata{
		Int64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "execution_count",
				MetricColumnName: "EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		Float64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "avg_latency_seconds",
				MetricColumnName: "AVG_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "seconds",
			},
		},

		Float64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "avg_rows",
				MetricColumnName: "AVG_ROWS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		Float64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "avg_bytes",
				MetricColumnName: "AVG_BYTES",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "bytes",
			},
		},

		Float64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "avg_rows_scanned",
				MetricColumnName: "AVG_ROWS_SCANNED",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		Float64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "avg_cpu_seconds",
				MetricColumnName: "AVG_CPU_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "seconds",
			},
		},

		Int64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "all_failed_execution_count",
				MetricColumnName: "ALL_FAILED_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		Float64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "all_failed_avg_latency_seconds",
				MetricColumnName: "ALL_FAILED_AVG_LATENCY_SECONDS",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "seconds",
			},
		},

		Int64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "cancelled_or_disconnected_execution_count",
				MetricColumnName: "CANCELLED_OR_DISCONNECTED_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},

		Int64MetricValueMetadata{
			QueryMetricValueMetadata{
				MetricName:       "timed_out_execution_count",
				MetricColumnName: "TIMED_OUT_EXECUTION_COUNT",
				MetricDataType:   pdata.MetricDataTypeGauge,
				MetricUnit:       "one",
			},
		},
	}

	return &MetricsReaderMetadata{
		Name:                      "top minute query stats",
		projectId:                 projectId,
		instanceId:                instanceId,
		databaseName:              databaseName,
		Query:                     query,
		TimestampColumnName:       timestampColumnName,
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}
