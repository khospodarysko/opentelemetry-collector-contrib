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

package datasource

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

type MetricsSource struct {
	client          *spanner.Client
	metricsSourceId *MetricsSourceId
}

func (metricsSource *MetricsSource) Client() *spanner.Client {
	return metricsSource.client
}

func (metricsSource *MetricsSource) MetricsSourceId() *MetricsSourceId {
	return metricsSource.metricsSourceId
}

func NewMetricsSource(ctx context.Context, metricsSourceId *MetricsSourceId,
	credentialsFilePath string) (*MetricsSource, error) {

	credentialsFileClientOption := option.WithCredentialsFile(credentialsFilePath)
	client, err := spanner.NewClient(ctx, metricsSourceId.Id(), credentialsFileClientOption)

	if err != nil {
		return nil, err
	}

	metricsSource := &MetricsSource{
		client:          client,
		metricsSourceId: metricsSourceId,
	}

	return metricsSource, nil
}

func NewMetricsSourceFromClient(client *spanner.Client, metricsSourceId *MetricsSourceId) *MetricsSource {
	return &MetricsSource{
		client:          client,
		metricsSourceId: metricsSourceId,
	}
}
