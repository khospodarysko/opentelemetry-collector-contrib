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
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

var _ component.MetricsReceiver = (*googleCloudSpannerReceiver)(nil)

type googleCloudSpannerReceiver struct {
	logger                *zap.Logger
	nextConsumer          consumer.Metrics
	config                *Config
	projectMetricsReaders []*ProjectMetricsReader
}

func newGoogleCloudSpannerReceiver(
	logger *zap.Logger,
	config *Config,
	nextConsumer consumer.Metrics) (component.MetricsReceiver, error) {

	r := &googleCloudSpannerReceiver{
		logger:       logger,
		nextConsumer: nextConsumer,
		config:       config,
	}
	return r, nil
}

func (gcsReceiver *googleCloudSpannerReceiver) Start(ctx context.Context, host component.Host) error {
	err := gcsReceiver.initializeProjectMetricsReaders(ctx)

	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(gcsReceiver.config.CollectionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				gcsReceiver.collectData(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (gcsReceiver *googleCloudSpannerReceiver) Shutdown(context.Context) error {
	for _, reader := range gcsReceiver.projectMetricsReaders {
		reader.Shutdown()
	}

	return nil
}

func (gcsReceiver *googleCloudSpannerReceiver) initializeProjectMetricsReaders(ctx context.Context) error {
	gcsReceiver.projectMetricsReaders = make([]*ProjectMetricsReader, len(gcsReceiver.config.Projects))

	for i, project := range gcsReceiver.config.Projects {
		reader, err := NewProjectMetricsReader(project, gcsReceiver.config.TopMetricsQueryMaxRows, ctx, gcsReceiver.logger)

		if err != nil {
			return err
		}

		gcsReceiver.projectMetricsReaders[i] = reader
	}

	return nil
}

func (gcsReceiver *googleCloudSpannerReceiver) collectData(ctx context.Context) error {
	var allMetrics []pdata.Metrics

	for _, metricsReader := range gcsReceiver.projectMetricsReaders {
		allMetrics = append(allMetrics, metricsReader.ReadMetrics(ctx)...)
	}

	for _, metric := range allMetrics {
		err2 := gcsReceiver.nextConsumer.ConsumeMetrics(ctx, metric)
		if err2 != nil {
			return err2
		}
	}

	return nil
}
