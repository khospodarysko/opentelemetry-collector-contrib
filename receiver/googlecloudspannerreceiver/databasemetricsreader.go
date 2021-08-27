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
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/collector/model/pdata"

	"cloud.google.com/go/spanner"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type DatabaseMetricsReader struct {
	client           *spanner.Client
	fullDatabaseName string
	logger           *zap.Logger
}

func NewDatabaseMetricsReader(ctx context.Context, projectId string, instanceId string, databaseName string,
	serviceAccountPath string, logger *zap.Logger) (*DatabaseMetricsReader, error) {
	fullDatabaseName := createFullDatabaseName(projectId, instanceId, databaseName)
	client, err := createClient(ctx, fullDatabaseName, serviceAccountPath, logger)

	if err != nil {
		return nil, err
	}

	return &DatabaseMetricsReader{
		client:           client,
		fullDatabaseName: fullDatabaseName,
		logger:           logger,
	}, nil
}

func createClient(ctx context.Context, database string, serviceAccountPath string, logger *zap.Logger) (*spanner.Client, error) {
	serviceAccountClientOption := option.WithCredentialsFile(serviceAccountPath)
	client, err := spanner.NewClient(ctx, database, serviceAccountClientOption)

	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred during client instantiation for database %v", database))
		return nil, err
	}

	return client, nil
}

func createFullDatabaseName(projectId string, instanceId string, database string) string {
	return "projects/" + projectId + "/instances/" + instanceId + "/databases/" + database
}

func (reader *DatabaseMetricsReader) Shutdown() {
	reader.logger.Info(fmt.Sprintf("Closing connection to database %v", reader.fullDatabaseName))
	reader.client.Close()
}

func (reader *DatabaseMetricsReader) ReadMetrics(ctx context.Context) []pdata.Metrics {
	reader.logger.Info(fmt.Sprintf("Executing read method for database %v", reader.fullDatabaseName))

	var result []pdata.Metrics

	stmt := spanner.Statement{SQL: "SELECT interval_end, execution_count FROM spanner_sys.query_stats_total_minute WHERE interval_end = (SELECT MAX(interval_end) FROM spanner_sys.query_stats_top_minute);"}
	rowsIterator := reader.client.Single().Query(ctx, stmt)
	defer rowsIterator.Stop()

	for {
		row, err := rowsIterator.Next()
		if err == iterator.Done {
			fmt.Println("Done")
			return result
		}
		if err != nil {
			log.Fatalf("Query failed with %v", err)
			return result
		}

		var statsTotalMinute QueryStatsTotalMinute
		err = row.ToStruct(&statsTotalMinute)
		if err != nil {
			log.Fatalf("ToStruct transform failed with %v", err)
			return result
		}
		fmt.Printf("Got value %v\n", statsTotalMinute)
		result = append(result, statsTotalMinute.ConvertToExecutionCountMetrics(reader.client.DatabaseName()))
	}

	return result
}

type QueryStatsTotalMinute struct {
	IntervalEnd    time.Time `spanner:"interval_end"`
	ExecutionCount int64     `spanner:"execution_count"`
}

func (v *QueryStatsTotalMinute) ConvertToExecutionCountMetrics(dbname string) pdata.Metrics {
	fmt.Printf("Struct values - IntervalEnd:%v ExecutionCount:%v\n", v.IntervalEnd, v.ExecutionCount)

	md := pdata.NewMetrics()
	rms := md.ResourceMetrics()
	rm := rms.AppendEmpty()
	//resource := rm.Resource()
	//resource.Attributes().UpsertString("koskey", "kosvalue")

	ilms := rm.InstrumentationLibraryMetrics()
	ilm := ilms.AppendEmpty()
	metric := ilm.Metrics().AppendEmpty()
	metric.SetName("lohika_gcp_total_minute_execution_count")
	metric.SetUnit("one")
	metric.SetDataType(pdata.MetricDataTypeGauge)
	intGauge := metric.Gauge()
	dataPoints := intGauge.DataPoints()
	dataPoint := dataPoints.AppendEmpty()

	dataPoint.SetIntVal(v.ExecutionCount)
	dataPoint.SetTimestamp(pdata.TimestampFromTime(time.Now()))
	// TODO find a way to set labels
	//labels := pdata.NewStringMap()
	//labels.Insert("dbname", dbname)
	//labels.CopyTo(dataPoint.LabelsMap())

	return md
}
