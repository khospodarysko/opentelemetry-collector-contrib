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

package statsreader

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/spannertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	spannerDatabaseName = "projects/" + projectID + "/instances/" + instanceID + "/databases/" + databaseName
	maxRowsLimit        = 1
)

func createMetricsMetadata(query string) *metadata.MetricsMetadata {
	// Labels
	queryLabelValuesMetadata := []metadata.LabelValueMetadata{
		metadata.NewStringLabelValueMetadata("metric_label", "METRIC_LABEL"),
	}

	// Metrics
	queryMetricValuesMetadata := []metadata.MetricValueMetadata{
		metadata.NewInt64MetricValueMetadata("metric_value", "METRIC_VALUE",
			pdata.MetricDataTypeGauge, "unit"),
	}

	return &metadata.MetricsMetadata{
		Name:                      "test stats",
		Query:                     query,
		MetricNamePrefix:          "test_stats/",
		TimestampColumnName:       "INTERVAL_END",
		QueryLabelValuesMetadata:  queryLabelValuesMetadata,
		QueryMetricValuesMetadata: queryMetricValuesMetadata,
	}
}

func createCurrentStatsReader(client *spanner.Client) Reader {
	query := "SELECT * FROM STATS"
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	databaseFromClient := datasource.NewDatabaseFromClient(client, databaseID)

	return newCurrentStatsReader(zap.NewNop(), databaseFromClient, createMetricsMetadata(query), ReaderConfig{})
}

func createCurrentStatsReaderWithMaxRowsLimit(client *spanner.Client) Reader {
	query := "SELECT * FROM STATS"
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	databaseFromClient := datasource.NewDatabaseFromClient(client, databaseID)
	config := ReaderConfig{
		TopMetricsQueryMaxRows: maxRowsLimit,
	}

	return newCurrentStatsReader(zap.NewNop(), databaseFromClient, createMetricsMetadata(query), config)
}

func createIntervalStatsReader(client *spanner.Client, backfillEnabled bool) Reader {
	query := "SELECT * FROM STATS WHERE INTERVAL_END = @pullTimestamp"
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	databaseFromClient := datasource.NewDatabaseFromClient(client, databaseID)
	config := ReaderConfig{
		BackfillEnabled: backfillEnabled,
	}

	return newIntervalStatsReader(zap.NewNop(), databaseFromClient, createMetricsMetadata(query), config)
}

func createIntervalStatsReaderWithMaxRowsLimit(client *spanner.Client, backfillEnabled bool) Reader {
	query := "SELECT * FROM STATS WHERE INTERVAL_END = @pullTimestamp"
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	databaseFromClient := datasource.NewDatabaseFromClient(client, databaseID)
	config := ReaderConfig{
		TopMetricsQueryMaxRows: maxRowsLimit,
		BackfillEnabled:        backfillEnabled,
	}

	return newIntervalStatsReader(zap.NewNop(), databaseFromClient, createMetricsMetadata(query), config)
}

func TestStatsReaders_Read(t *testing.T) {
	timestamp := shiftToStartOfMinute(time.Now().UTC())
	ctx := context.Background()
	server, err := spannertest.NewServer(":0")
	require.Nil(t, err)
	defer server.Close()

	conn, err := grpc.Dial(server.Addr, grpc.WithInsecure())
	require.Nil(t, err)

	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	require.Nil(t, err)
	defer func(databaseAdminClient *database.DatabaseAdminClient) {
		err := databaseAdminClient.Close()
		if err != nil {
			// Ignore it
		}
	}(databaseAdminClient)

	op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{`CREATE TABLE STATS (
			INTERVAL_END TIMESTAMP,
			METRIC_LABEL STRING(MAX),
			METRIC_VALUE INT64
		) PRIMARY KEY (METRIC_LABEL)
		`},
	})
	require.Nil(t, err)

	err = op.Wait(ctx)
	require.Nil(t, err)

	databaseClient, err := spanner.NewClient(ctx, spannerDatabaseName, option.WithGRPCConn(conn))
	require.Nil(t, err)
	defer databaseClient.Close()

	_, err = databaseClient.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("STATS",
			[]string{"INTERVAL_END", "METRIC_LABEL", "METRIC_VALUE"},
			[]interface{}{timestamp, "Qwerty", 10}),
		spanner.Insert("STATS",
			[]string{"INTERVAL_END", "METRIC_LABEL", "METRIC_VALUE"},
			[]interface{}{timestamp.Add(-1 * time.Minute), "Test", 20}),
		spanner.Insert("STATS",
			[]string{"INTERVAL_END", "METRIC_LABEL", "METRIC_VALUE"},
			[]interface{}{timestamp.Add(-1 * time.Minute), "Spanner", 30}),
	})

	require.Nil(t, err)

	testCases := map[string]struct {
		reader                Reader
		expectedMetricsAmount int
	}{
		"Current stats reader without max rows limit":                   {createCurrentStatsReader(databaseClient), 3},
		"Current stats reader with max rows limit":                      {createCurrentStatsReaderWithMaxRowsLimit(databaseClient), 1},
		"Interval stats reader without backfill without max rows limit": {createIntervalStatsReader(databaseClient, false), 1},
		"Interval stats reader without backfill with max rows limit":    {createIntervalStatsReaderWithMaxRowsLimit(databaseClient, false), 1},
		"Interval stats reader with backfill without max rows limit":    {createIntervalStatsReader(databaseClient, true), 3},
		"Interval stats reader with backfill with max rows limit":       {createIntervalStatsReaderWithMaxRowsLimit(databaseClient, true), 2},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			metrics, err := testCase.reader.Read(ctx)

			require.Nil(t, err)
			assert.Equal(t, testCase.expectedMetricsAmount, len(metrics))
		})
	}
}
