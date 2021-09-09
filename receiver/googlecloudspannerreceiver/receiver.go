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
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/datasource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/reader"
)

var _ component.MetricsReceiver = (*googleCloudSpannerReceiver)(nil)

type googleCloudSpannerReceiver struct {
	logger                *zap.Logger
	nextConsumer          consumer.Metrics
	config                *Config
	cancel                context.CancelFunc
	projectMetricsReaders []*reader.ProjectMetricsReader
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
	ctx, gcsReceiver.cancel = context.WithCancel(ctx)
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
				// TODO Need to add appropriate error handling here
				gcsReceiver.collectData(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (gcsReceiver *googleCloudSpannerReceiver) Shutdown(context.Context) error {
	for _, metricsReader := range gcsReceiver.projectMetricsReaders {
		metricsReader.Shutdown()
	}

	gcsReceiver.cancel()

	return nil
}

func (gcsReceiver *googleCloudSpannerReceiver) initializeProjectMetricsReaders(ctx context.Context) error {
	gcsReceiver.projectMetricsReaders = make([]*reader.ProjectMetricsReader, len(gcsReceiver.config.Projects))

	for i, project := range gcsReceiver.config.Projects {
		metricsReader, err := newProjectMetricsReader(project, gcsReceiver.config.TopMetricsQueryMaxRows, ctx, gcsReceiver.logger)

		if err != nil {
			return err
		}

		gcsReceiver.projectMetricsReaders[i] = metricsReader
	}

	return nil
}

func newProjectMetricsReader(project Project, topMetricsQueryMaxRows int, ctx context.Context,
	logger *zap.Logger) (*reader.ProjectMetricsReader, error) {
	logger.Info(fmt.Sprintf("Constructing project metrics reader for project id %v", project.ID))

	var databaseMetricsReaders []*reader.DatabaseMetricsReader

	for _, instance := range project.Instances {
		for _, database := range instance.Databases {
			logger.Info(fmt.Sprintf("Constructing database metrics reader for project id %v, instance id %v, database %v",
				project.ID, instance.ID, database.Name))

			metricsSourceId := datasource.NewMetricsSourceId(project.ID, instance.ID, database.Name)

			databaseMetricsReader, err := reader.NewDatabaseMetricsReader(ctx, metricsSourceId,
				project.ServiceAccountKey, topMetricsQueryMaxRows, logger)

			if err != nil {
				return nil, err
			}

			databaseMetricsReaders = append(databaseMetricsReaders, databaseMetricsReader)
		}
	}

	return reader.NewProjectMetricsReader(databaseMetricsReaders, logger), nil
}

func (gcsReceiver *googleCloudSpannerReceiver) collectData(ctx context.Context) error {
	var allMetrics []pdata.Metrics

	for _, metricsReader := range gcsReceiver.projectMetricsReaders {
		allMetrics = append(allMetrics, metricsReader.ReadMetrics(ctx)...)
	}

	for _, metric := range allMetrics {
		err := gcsReceiver.nextConsumer.ConsumeMetrics(ctx, metric)
		if err != nil {
			return err
		}
	}

	return nil
}
