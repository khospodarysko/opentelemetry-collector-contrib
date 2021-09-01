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
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
)

type testReader struct {
	throwError bool
}

func (tr testReader) Name() string {
	return "testReader"
}

func (tr testReader) Read(ctx context.Context) ([]pdata.Metrics, error) {
	if tr.throwError {
		return nil, fmt.Errorf("error")
	}

	return []pdata.Metrics{{}}, nil
}

func TestNewDatabaseReader(t *testing.T) {
	ctx := context.Background()
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	serviceAccountPath := "../../testdata/serviceAccount.json"
	readerConfig := ReaderConfig{
		TopMetricsQueryMaxRows: topMetricsQueryMaxRows,
		BackfillEnabled:        false,
	}
	logger := zap.NewNop()

	reader, err := NewDatabaseReader(ctx, databaseID, serviceAccountPath, readerConfig, logger)

	assert.Nil(t, err)
	assert.Equal(t, databaseID, reader.database.DatabaseID())
	assert.Equal(t, logger, reader.logger)
	assert.Equal(t, 9, len(reader.readers))
}

func TestNewDatabaseReaderWithError(t *testing.T) {
	ctx := context.Background()
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	serviceAccountPath := "does not exist"
	readerConfig := ReaderConfig{
		TopMetricsQueryMaxRows: topMetricsQueryMaxRows,
		BackfillEnabled:        false,
	}
	logger := zap.NewNop()

	reader, err := NewDatabaseReader(ctx, databaseID, serviceAccountPath, readerConfig, logger)

	assert.NotNil(t, err)
	assert.Nil(t, reader)
}

func TestDatabaseReader_Name(t *testing.T) {
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, databaseName)
	database := datasource.NewDatabaseFromClient(client, databaseID)
	logger := zap.NewNop()

	reader := DatabaseReader{
		logger:   logger,
		database: database,
	}

	assert.Equal(t, database.DatabaseID().ID(), reader.Name())
}

func TestDatabaseReader_Shutdown(t *testing.T) {
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, databaseName)
	database := datasource.NewDatabaseFromClient(client, databaseID)
	logger := zap.NewNop()

	reader := DatabaseReader{
		logger:   logger,
		database: database,
	}

	defer func() {
		if err := recover(); err != nil {
			// Ignoring because it was expected in case of not real spanner database client
		}
	}()

	reader.Shutdown()
}

func TestDatabaseReader_Read(t *testing.T) {
	databaseID := datasource.NewDatabaseID(projectID, instanceID, databaseName)
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, databaseName)
	database := datasource.NewDatabaseFromClient(client, databaseID)
	logger := zap.NewNop()

	testReaderThrowNoError := testReader{
		throwError: false,
	}

	testReaderThrowError := testReader{
		throwError: true,
	}

	testCases := map[string]struct {
		readers              []Reader
		expectedMetricsCount int
	}{
		"Read with no error": {[]Reader{testReaderThrowNoError}, 1},
		"Read with error":    {[]Reader{testReaderThrowError}, 0},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reader := DatabaseReader{
				logger:   logger,
				database: database,
				readers:  testCase.readers,
			}

			metrics := reader.Read(ctx)

			assert.Equal(t, testCase.expectedMetricsCount, len(metrics))
		})
	}
}
