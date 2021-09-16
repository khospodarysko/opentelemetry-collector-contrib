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
	"fmt"
	"time"

	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/metadata"
)

const (
	backFillIntervalDuration = time.Hour
)

type intervalStatsReader struct {
	currentStatsReader
	backFillEnabled   bool
	lastPullTimestamp time.Time
}

func newIntervalStatsReaderWithMaxRowsLimit(
	logger *zap.Logger,
	database *datasource.Database,
	metricsMetadata *metadata.MetricsMetadata,
	backFillEnabled bool,
	topMetricsQueryMaxRows int) *intervalStatsReader {

	reader := currentStatsReader{
		logger:                 logger,
		database:               database,
		metricsMetadata:        metricsMetadata,
		topMetricsQueryMaxRows: topMetricsQueryMaxRows,
		statement:              intervalStatsStatement,
	}

	return &intervalStatsReader{
		currentStatsReader: reader,
		backFillEnabled:    backFillEnabled,
	}
}

func newIntervalStatsReader(
	logger *zap.Logger,
	database *datasource.Database,
	metricsMetadata *metadata.MetricsMetadata,
	backFillEnabled bool) *intervalStatsReader {

	reader := currentStatsReader{
		logger:          logger,
		database:        database,
		metricsMetadata: metricsMetadata,
		statement:       intervalStatsStatement,
	}

	return &intervalStatsReader{
		currentStatsReader: reader,
		backFillEnabled:    backFillEnabled,
	}
}

func (reader *intervalStatsReader) Read(ctx context.Context) ([]pdata.Metrics, error) {
	reader.logger.Info(fmt.Sprintf("Executing read method for reader %v", reader.Name()))

	// Generating pull timestamps
	pullTimestamps := pullTimestamps(reader.lastPullTimestamp, reader.backFillEnabled)

	var collectedMetrics []pdata.Metrics

	// Pulling metrics for each generated pull timestamp
	for _, pullTimestamp := range pullTimestamps {
		stmtArgs := statementArgs{
			query:                  reader.metricsMetadata.Query,
			topMetricsQueryMaxRows: reader.topMetricsQueryMaxRows,
			pullTimestamp:          pullTimestamp,
		}

		stmt := reader.statement(stmtArgs)
		metrics, err := reader.pull(ctx, stmt)

		if err != nil {
			return nil, err
		}

		collectedMetrics = append(collectedMetrics, metrics...)
	}

	reader.lastPullTimestamp = pullTimestamps[len(pullTimestamps)-1]

	return collectedMetrics, nil
}

// This slice will always contain at least one value.
func pullTimestamps(lastPullTimestamp time.Time, backFillEnabled bool) []time.Time {
	var timestamps []time.Time
	upperBound := nowAtStartOfMinute()

	if lastPullTimestamp.IsZero() {
		if backFillEnabled {
			timestamps = pullTimestampsWithMinuteDifference(upperBound.Add(-1*backFillIntervalDuration), upperBound)
		} else {
			timestamps = []time.Time{upperBound}
		}
	} else {
		// lastPullTimestamp is already set to start of minute
		timestamps = pullTimestampsWithMinuteDifference(lastPullTimestamp, upperBound)
	}

	return timestamps
}

// This slice will always contain at least one value.
// Difference between each two points is 1 minute.
func pullTimestampsWithMinuteDifference(lowerBound time.Time, upperBound time.Time) []time.Time {
	var timestamps []time.Time

	for value := lowerBound.Add(time.Minute); !value.After(upperBound); value = value.Add(time.Minute) {
		timestamps = append(timestamps, value)
	}

	// To ensure that we did not miss upper bound and timestamps slice will contain at least one value
	if len(timestamps) <= 0 || timestamps[len(timestamps)-1] != upperBound {
		timestamps = append(timestamps, upperBound)
	}

	return timestamps
}

func nowAtStartOfMinute() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
}
