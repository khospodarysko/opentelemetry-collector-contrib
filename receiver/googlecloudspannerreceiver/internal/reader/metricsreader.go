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
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	projectIdLabelName  = "project_id"
	instanceIdLabelName = "instance_id"
	databaseLabelName   = "database"

	topMetricsQueryLimitParameterName = "topMetricsQueryMaxRows"
	topMetricsQueryLimitCondition     = " LIMIT @" + topMetricsQueryLimitParameterName
)

/* ------------------------------------------------------------------------------------------------------------------ */

type MetricsReader struct {
	Name                   string
	ProjectId              string
	InstanceId             string
	DatabaseName           string
	Query                  string
	TopMetricsQueryMaxRows int
	MetricNamePrefix       string
	TimestampColumnName    string
	// In addition to common metric labels
	QueryLabelValuesMetadata  []metadata.LabelValueMetadata
	QueryMetricValuesMetadata []metadata.MetricValueMetadata
}

/* ------------------------------------------------------------------------------------------------------------------ */

func (metricsReader *MetricsReader) FullName() string {
	return metricsReader.Name + " " + metricsReader.ProjectId + "::" + metricsReader.InstanceId + "::" + metricsReader.DatabaseName
}

func (metricsReader *MetricsReader) Read(ctx context.Context, client *spanner.Client, logger *zap.Logger) ([]pdata.Metrics, error) {
	logger.Info(fmt.Sprintf("Executing read method for metrics reader %v", metricsReader.FullName()))

	stmt := spanner.Statement{SQL: metricsReader.Query}

	if metricsReader.TopMetricsQueryMaxRows > 0 {
		stmt = spanner.Statement{
			SQL: metricsReader.Query + topMetricsQueryLimitCondition,
			Params: map[string]interface{}{
				topMetricsQueryLimitParameterName: metricsReader.TopMetricsQueryMaxRows,
			}}
	}

	rowsIterator := client.Single().Query(ctx, stmt)
	defer rowsIterator.Stop()

	var collectedMetrics []pdata.Metrics

	for {
		row, err := rowsIterator.Next()

		if err != nil {
			if err == iterator.Done {
				break
			}

			logger.Error(fmt.Sprintf("Query \"%v\" failed with %v", stmt.SQL, err))

			return nil, err
		}

		rowMetrics, err := metricsReader.rowToMetrics(row)

		if err != nil {
			logger.Error(fmt.Sprintf("Query \"%v\" failed with %v", stmt.SQL, err))
			return nil, err
		}

		collectedMetrics = append(collectedMetrics, rowMetrics...)
	}

	return collectedMetrics, nil
}

func (metricsReader *MetricsReader) intervalEnd(row *spanner.Row) (time.Time, error) {
	var intervalEnd time.Time
	var err error

	if metricsReader.TimestampColumnName != "" {
		err = row.ColumnByName(metricsReader.TimestampColumnName, &intervalEnd)
	} else {
		intervalEnd = time.Now().UTC()
	}

	return intervalEnd, err
}

func (metricsReader *MetricsReader) toLabelValues(row *spanner.Row) ([]metadata.LabelValue, error) {
	values := make([]metadata.LabelValue, len(metricsReader.QueryLabelValuesMetadata))

	for i, metadataItems := range metricsReader.QueryLabelValuesMetadata {
		var err error

		if values[i], err = toLabelValue(metadataItems, row); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func toLabelValue(labelValueMetadata metadata.LabelValueMetadata, row *spanner.Row) (metadata.LabelValue, error) {
	valueHolder := labelValueMetadata.ValueHolder()

	err := row.ColumnByName(labelValueMetadata.GetLabelColumnName(), valueHolder)

	var value metadata.LabelValue = nil

	switch labelValueMetadataCasted := labelValueMetadata.(type) {
	case metadata.StringLabelValueMetadata:
		value = metadata.NewStringLabelValue(labelValueMetadataCasted, valueHolder)
	case metadata.Int64LabelValueMetadata:
		value = metadata.NewInt64LabelValue(labelValueMetadataCasted, valueHolder)
	case metadata.BoolLabelValueMetadata:
		value = metadata.NewBoolLabelValue(labelValueMetadataCasted, valueHolder)
	case metadata.StringSliceLabelValueMetadata:
		value = metadata.NewStringSliceLabelValue(labelValueMetadataCasted, valueHolder)
	case metadata.ByteSliceLabelValueMetadata:
		value = metadata.NewByteSliceLabelValue(labelValueMetadataCasted, valueHolder)
	}

	return value, err
}

func (metricsReader *MetricsReader) toMetricValues(row *spanner.Row) ([]metadata.MetricValue, error) {
	values := make([]metadata.MetricValue, len(metricsReader.QueryMetricValuesMetadata))

	for i, metadataItems := range metricsReader.QueryMetricValuesMetadata {
		var err error

		if values[i], err = toMetricValue(metadataItems, row); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func toMetricValue(metricValueMetadata metadata.MetricValueMetadata, row *spanner.Row) (metadata.MetricValue, error) {
	valueHolder := metricValueMetadata.ValueHolder()

	err := row.ColumnByName(metricValueMetadata.GetMetricColumnName(), valueHolder)

	var value metadata.MetricValue = nil

	switch metricValueMetadataCasted := metricValueMetadata.(type) {
	case metadata.Int64MetricValueMetadata:
		value = metadata.NewInt64MetricValue(metricValueMetadataCasted, valueHolder)
	case metadata.Float64MetricValueMetadata:
		value = metadata.NewFloat64MetricValue(metricValueMetadataCasted, valueHolder)
	}

	return value, err
}

func (metricsReader *MetricsReader) rowToMetrics(row *spanner.Row) ([]pdata.Metrics, error) {
	intervalEnd, err := metricsReader.intervalEnd(row)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error occurred during extracting interval end %v", err))
	}

	// Reading labels
	labelValues, err := metricsReader.toLabelValues(row)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error occurred during extracting label values for row: %v", err))
	}

	// Reading metrics
	metricValues, err := metricsReader.toMetricValues(row)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error occurred during extracting metric values row: %v", err))
	}

	return metricsReader.toMetrics(intervalEnd, labelValues, metricValues), nil
}

func (metricsReader *MetricsReader) toMetrics(intervalEnd time.Time, labelValues []metadata.LabelValue,
	metricValues []metadata.MetricValue) []pdata.Metrics {

	var metrics []pdata.Metrics

	for _, metricValue := range metricValues {
		md := pdata.NewMetrics()
		rms := md.ResourceMetrics()
		rm := rms.AppendEmpty()

		ilms := rm.InstrumentationLibraryMetrics()
		ilm := ilms.AppendEmpty()
		metric := ilm.Metrics().AppendEmpty()
		metric.SetName(metricsReader.MetricNamePrefix + metricValue.GetMetricName())
		metric.SetUnit(metricValue.GetMetricUnit())
		metric.SetDataType(metricValue.GetMetricDataType())

		var dataPoints pdata.NumberDataPointSlice

		switch metricValue.GetMetricDataType() {
		case pdata.MetricDataTypeGauge:
			dataPoints = metric.Gauge().DataPoints()
		case pdata.MetricDataTypeSum:
			dataPoints = metric.Sum().DataPoints()
		}

		dataPoint := dataPoints.AppendEmpty()

		switch valueCasted := metricValue.(type) {
		case metadata.Float64MetricValue:
			dataPoint.SetDoubleVal(valueCasted.Value)
		case metadata.Int64MetricValue:
			dataPoint.SetIntVal(valueCasted.Value)
		}

		dataPoint.SetTimestamp(pdata.NewTimestampFromTime(intervalEnd))

		for _, labelValue := range labelValues {
			switch valueCasted := labelValue.(type) {
			case metadata.StringLabelValue:
				dataPoint.Attributes().InsertString(valueCasted.LabelName, valueCasted.Value)
			case metadata.BoolLabelValue:
				dataPoint.Attributes().InsertBool(valueCasted.LabelName, valueCasted.Value)
			case metadata.Int64LabelValue:
				dataPoint.Attributes().InsertInt(valueCasted.LabelName, valueCasted.Value)
			case metadata.StringSliceLabelValue:
				dataPoint.Attributes().InsertString(valueCasted.LabelName, valueCasted.Value)
			case metadata.ByteSliceLabelValue:
				dataPoint.Attributes().InsertString(valueCasted.LabelName, valueCasted.Value)
			}
		}

		dataPoint.Attributes().InsertString(projectIdLabelName, metricsReader.ProjectId)
		dataPoint.Attributes().InsertString(instanceIdLabelName, metricsReader.InstanceId)
		dataPoint.Attributes().InsertString(databaseLabelName, metricsReader.DatabaseName)

		metrics = append(metrics, md)
	}

	return metrics
}
