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

package statsmetadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsReaderMetadataHolder(t *testing.T) {
	holder := MetricsMetadataHolder()

	assert.NotNil(t, holder)

	assert.Equal(t, holder, MetricsMetadataHolder())

	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeQueryStatsTop))
	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeQueryStatsTotal))

	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeReadStatsTop))
	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeReadStatsTotal))

	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeTransactionStatsTop))
	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeTransactionStatsTotal))

	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeLockStatsTop))
	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeLockStatsTotal))

	assert.NotNil(t, holder.MetricsMetadata(MetricsMetadataTypeActiveQueriesSummary))
}
