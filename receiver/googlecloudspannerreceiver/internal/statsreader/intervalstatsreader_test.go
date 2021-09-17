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
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")
	database := datasource.NewDatabaseFromClient(client, databaseID)
	metricsMetadata := &metadata.MetricsMetadata{
		Name: name,
	}

	reader := intervalStatsReader{
		currentStatsReader: currentStatsReader{
			database:        database,
			metricsMetadata: metricsMetadata,
		},
	}

	assert.Equal(t, reader.metricsMetadata.Name+" "+databaseID.ProjectID()+"::"+
		databaseID.InstanceID()+"::"+databaseID.DatabaseName(), reader.Name())
}

func TestNewIntervalStatsReader(t *testing.T) {
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "")
	database := datasource.NewDatabaseFromClient(client, databaseID)
	metricsMetadata := &metadata.MetricsMetadata{
		Name: name,
	}
	logger := zap.NewNop()
	config := ReaderConfig{
		TopMetricsQueryMaxRows: topMetricsQueryMaxRows,
		BackfillEnabled:        true,
	}

	reader := newIntervalStatsReader(logger, database, metricsMetadata, config)

	assert.Equal(t, database, reader.database)
	assert.Equal(t, logger, reader.logger)
	assert.Equal(t, metricsMetadata, reader.metricsMetadata)
	assert.Equal(t, topMetricsQueryMaxRows, reader.topMetricsQueryMaxRows)
	assert.True(t, reader.backfillEnabled)
}

func TestIntervalStatsReader_NewPullStatement(t *testing.T) {
	timestamp := time.Now().UTC()
	metricsMetadata := &metadata.MetricsMetadata{
		Query: query,
	}
	reader := intervalStatsReader{
		currentStatsReader: currentStatsReader{
			metricsMetadata:        metricsMetadata,
			topMetricsQueryMaxRows: topMetricsQueryMaxRows,
			statement:              intervalStatsStatement,
		},
	}

	assert.NotZero(t, reader.newPullStatement(timestamp))
}

func TestPullTimestamps(t *testing.T) {
	nowAtStartOfMinute := nowAtStartOfMinute()
	backfillIntervalAgo := nowAtStartOfMinute.Add(-1 * backfillIntervalDuration)
	backfillIntervalAgoWthSomeSeconds := backfillIntervalAgo.Add(-15 * time.Second)

	testCases := map[string]struct {
		lastPullTimestamp  time.Time
		backfillEnabled    bool
		amountOfTimestamps int
	}{
		"Zero last pull timestamp without backfill":                                                       {time.Time{}, false, 1},
		"Zero last pull timestamp with backfill":                                                          {time.Time{}, true, int(backfillIntervalDuration.Minutes())},
		"Last pull timestamp now at start of minute backfill does not matter":                             {nowAtStartOfMinute, false, 1},
		"Last pull timestamp back fill interval ago of minute backfill does not matter":                   {backfillIntervalAgo, false, int(backfillIntervalDuration.Minutes())},
		"Last pull timestamp back fill interval ago with some seconds of minute backfill does not matter": {backfillIntervalAgoWthSomeSeconds, false, int(backfillIntervalDuration.Minutes()) + 1},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			timestamps := pullTimestamps(testCase.lastPullTimestamp, testCase.backfillEnabled)

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
