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

import (
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
)

const (
	projectIDLabelName  = "project_id"
	instanceIDLabelName = "instance_id"
	databaseLabelName   = "database"
)

type MetricsMetadata struct {
	Name                string
	Query               string
	MetricNamePrefix    string
	TimestampColumnName string
	// In addition to common metric labels
	QueryLabelValuesMetadata  []LabelValueMetadata
	QueryMetricValuesMetadata []MetricValueMetadata
}

func (metadata *MetricsMetadata) timestamp(row *spanner.Row) (time.Time, error) {
	if metadata.TimestampColumnName == "" {
		return time.Now().UTC(), nil
	}
	var timestamp time.Time
	err := row.ColumnByName(metadata.TimestampColumnName, &timestamp)
	return timestamp, err
}

func (metadata *MetricsMetadata) toLabelValues(row *spanner.Row) ([]LabelValue, error) {
	values := make([]LabelValue, len(metadata.QueryLabelValuesMetadata))

	for i, metadataItems := range metadata.QueryLabelValuesMetadata {
		var err error

		if values[i], err = toLabelValue(metadataItems, row); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func toLabelValue(labelValueMetadata LabelValueMetadata, row *spanner.Row) (LabelValue, error) {
	valueHolder := labelValueMetadata.ValueHolder()

	err := row.ColumnByName(labelValueMetadata.ColumnName(), valueHolder)
	if err != nil {
		return nil, err
	}

	var value LabelValue = nil

	switch labelValueMetadataCasted := labelValueMetadata.(type) {
	case StringLabelValueMetadata:
		value = NewStringLabelValue(labelValueMetadataCasted, valueHolder)
	case Int64LabelValueMetadata:
		value = NewInt64LabelValue(labelValueMetadataCasted, valueHolder)
	case BoolLabelValueMetadata:
		value = NewBoolLabelValue(labelValueMetadataCasted, valueHolder)
	case StringSliceLabelValueMetadata:
		value = NewStringSliceLabelValue(labelValueMetadataCasted, valueHolder)
	case ByteSliceLabelValueMetadata:
		value = NewByteSliceLabelValue(labelValueMetadataCasted, valueHolder)
	}

	return value, nil
}

func (metadata *MetricsMetadata) toMetricValues(row *spanner.Row) ([]MetricValue, error) {
	values := make([]MetricValue, len(metadata.QueryMetricValuesMetadata))

	for i, metadataItems := range metadata.QueryMetricValuesMetadata {
		var err error

		if values[i], err = toMetricValue(metadataItems, row); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func toMetricValue(metricValueMetadata MetricValueMetadata, row *spanner.Row) (MetricValue, error) {
	valueHolder := metricValueMetadata.ValueHolder()

	err := row.ColumnByName(metricValueMetadata.ColumnName(), valueHolder)
	if err != nil {
		return nil, err
	}

	var value MetricValue = nil

	switch metricValueMetadataCasted := metricValueMetadata.(type) {
	case Int64MetricValueMetadata:
		value = NewInt64MetricValue(metricValueMetadataCasted, valueHolder)
	case Float64MetricValueMetadata:
		value = NewFloat64MetricValue(metricValueMetadataCasted, valueHolder)
	}

	return value, nil
}

func (metadata *MetricsMetadata) RowToMetrics(databaseID *datasource.DatabaseID, row *spanner.Row) ([]pdata.Metrics, error) {
	timestamp, err := metadata.timestamp(row)
	if err != nil {
		return nil, fmt.Errorf("error occurred during extracting timestamp %v", err)
	}

	// Reading labels
	labelValues, err := metadata.toLabelValues(row)
	if err != nil {
		return nil, fmt.Errorf("error occurred during extracting label values for row: %v", err)
	}

	// Reading metrics
	metricValues, err := metadata.toMetricValues(row)
	if err != nil {
		return nil, fmt.Errorf("error occurred during extracting metric values row: %v", err)
	}

	return metadata.toMetrics(databaseID, timestamp, labelValues, metricValues), nil
}

func (metadata *MetricsMetadata) toMetrics(databaseID *datasource.DatabaseID, timestamp time.Time,
	labelValues []LabelValue, metricValues []MetricValue) []pdata.Metrics {

	var metrics []pdata.Metrics

	for _, metricValue := range metricValues {
		md := pdata.NewMetrics()
		rms := md.ResourceMetrics()
		rm := rms.AppendEmpty()

		ilms := rm.InstrumentationLibraryMetrics()
		ilm := ilms.AppendEmpty()
		metric := ilm.Metrics().AppendEmpty()
		metric.SetName(metadata.MetricNamePrefix + metricValue.Name())
		metric.SetUnit(metricValue.Unit())
		metric.SetDataType(metricValue.DataType())

		var dataPoints pdata.NumberDataPointSlice

		switch metricValue.DataType() {
		case pdata.MetricDataTypeGauge:
			dataPoints = metric.Gauge().DataPoints()
		case pdata.MetricDataTypeSum:
			dataPoints = metric.Sum().DataPoints()
		}

		dataPoint := dataPoints.AppendEmpty()

		switch valueCasted := metricValue.(type) {
		case Float64MetricValue:
			dataPoint.SetDoubleVal(valueCasted.Val)
		case Int64MetricValue:
			dataPoint.SetIntVal(valueCasted.Val)
		}

		dataPoint.SetTimestamp(pdata.NewTimestampFromTime(timestamp))

		for _, labelValue := range labelValues {
			switch valueCasted := labelValue.(type) {
			case StringLabelValue:
				dataPoint.Attributes().InsertString(valueCasted.LabelName, valueCasted.Val)
			case BoolLabelValue:
				dataPoint.Attributes().InsertBool(valueCasted.LabelName, valueCasted.Val)
			case Int64LabelValue:
				dataPoint.Attributes().InsertInt(valueCasted.LabelName, valueCasted.Val)
			case StringSliceLabelValue:
				dataPoint.Attributes().InsertString(valueCasted.LabelName, valueCasted.Val)
			case ByteSliceLabelValue:
				dataPoint.Attributes().InsertString(valueCasted.LabelName, valueCasted.Val)
			}
		}

		dataPoint.Attributes().InsertString(projectIDLabelName, databaseID.ProjectID())
		dataPoint.Attributes().InsertString(instanceIDLabelName, databaseID.InstanceID())
		dataPoint.Attributes().InsertString(databaseLabelName, databaseID.DatabaseName())

		metrics = append(metrics, md)
	}

	return metrics
}
