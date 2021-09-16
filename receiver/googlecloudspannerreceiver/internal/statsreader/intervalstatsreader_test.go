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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

func TestIntervalStatsReader_Name(t *testing.T) {
	databaseId := datasource.NewDatabaseId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	database := datasource.NewDatabaseFromClient(client, databaseId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	reader := intervalStatsReader{
		currentStatsReader: currentStatsReader{
			database:        database,
			metricsMetadata: metricsMetadata,
		},
	}

	assert.Equal(t, reader.metricsMetadata.Name+" "+databaseId.ProjectId()+"::"+
		databaseId.InstanceId()+"::"+databaseId.DatabaseName(), reader.Name())
}

func TestNewIntervalStatsReaderWithMaxRowsLimit(t *testing.T) {
	databaseId := datasource.NewDatabaseId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	database := datasource.NewDatabaseFromClient(client, databaseId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	logger := zap.NewNop()

	reader := newIntervalStatsReaderWithMaxRowsLimit(logger, database, metricsMetadata, true, topMetricsQueryMaxRows)

	assert.Equal(t, database, reader.database)
	assert.Equal(t, logger, reader.logger)
	assert.Equal(t, metricsMetadata, reader.metricsMetadata)
	assert.Equal(t, topMetricsQueryMaxRows, reader.topMetricsQueryMaxRows)
	assert.True(t, reader.backFillEnabled)
}

func TestNewIntervalStatsReader(t *testing.T) {
	databaseId := datasource.NewDatabaseId(projectId, instanceId, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")

	database := datasource.NewDatabaseFromClient(client, databaseId)

	metricsMetadata := &metadata.MetricsMetadata{
		Name: "name",
	}

	logger := zap.NewNop()

	reader := newIntervalStatsReader(logger, database, metricsMetadata, true)

	assert.Equal(t, database, reader.database)
	assert.Equal(t, logger, reader.logger)
	assert.Equal(t, metricsMetadata, reader.metricsMetadata)
	assert.True(t, reader.backFillEnabled)
	assert.Equal(t, 0, reader.topMetricsQueryMaxRows)
}

func TestPullTimestamps(t *testing.T) {
	nowAtStartOfMinute := nowAtStartOfMinute()
	backFillIntervalAgo := nowAtStartOfMinute.Add(-1 * backFillIntervalDuration)
	backFillIntervalAgoWthSomeSeconds := backFillIntervalAgo.Add(-15 * time.Second)

	testCases := map[string]struct {
		lastPullTimestamp  time.Time
		backFillEnabled    bool
		amountOfTimestamps int
	}{
		"Zero last pull timestamp without back filling":                                                       {time.Time{}, false, 1},
		"Zero last pull timestamp with back filling":                                                          {time.Time{}, true, int(backFillIntervalDuration.Minutes())},
		"Last pull timestamp now at start of minute back filling does not matter":                             {nowAtStartOfMinute, false, 1},
		"Last pull timestamp back fill interval ago of minute back filling does not matter":                   {backFillIntervalAgo, false, int(backFillIntervalDuration.Minutes())},
		"Last pull timestamp back fill interval ago with some seconds of minute back filling does not matter": {backFillIntervalAgoWthSomeSeconds, false, int(backFillIntervalDuration.Minutes()) + 1},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			timestamps := pullTimestamps(testCase.lastPullTimestamp, testCase.backFillEnabled)

			assert.Equal(t, testCase.amountOfTimestamps, len(timestamps))
		})
	}
}

func TestPullTimestampsWithMinuteDifference(t *testing.T) {
	expectedAmountOfTimestamps := 5
	lowerBound := time.Date(2021, 9, 17, 16, 25, 0, 0, time.UTC)
	upperBound := lowerBound.Add(time.Duration(expectedAmountOfTimestamps) * time.Minute)

	timestamps := pullTimestampsWithMinuteDifference(lowerBound, upperBound)

	assert.Equal(t, expectedAmountOfTimestamps, len(timestamps))

	expectedTimestamp := lowerBound.Add(time.Minute)

	for _, timestamp := range timestamps {
		assert.Equal(t, expectedTimestamp, timestamp)
		expectedTimestamp = expectedTimestamp.Add(time.Minute)
	}

	// Check edge case: ensure that we didn't miss upperBound
	upperBound = lowerBound.Add(5 * time.Minute).Add(15 * time.Second)
	timestamps = pullTimestampsWithMinuteDifference(lowerBound, upperBound)

	assert.Equal(t, 6, len(timestamps))

	expectedTimestamp = lowerBound.Add(time.Minute)

	for i := 0; i < expectedAmountOfTimestamps; i++ {
		assert.Equal(t, expectedTimestamp, timestamps[i])
		expectedTimestamp = expectedTimestamp.Add(time.Minute)
	}

	assert.Equal(t, upperBound, timestamps[expectedAmountOfTimestamps])

}

func TestNowAtStartOfMinute(t *testing.T) {
	now := nowAtStartOfMinute()

	assert.Equal(t, 0, now.Second())
	assert.Equal(t, 0, now.Nanosecond())
}
