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
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver/internal/statsreader"
)

var _ component.MetricsReceiver = (*googleCloudSpannerReceiver)(nil)

type googleCloudSpannerReceiver struct {
	logger         *zap.Logger
	nextConsumer   consumer.Metrics
	config         *Config
	cancel         context.CancelFunc
	projectReaders []*statsreader.ProjectReader
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

func (receiver *googleCloudSpannerReceiver) Start(ctx context.Context, host component.Host) error {
	ctx, receiver.cancel = context.WithCancel(ctx)
	err := receiver.initializeProjectReaders(ctx)

	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(receiver.config.CollectionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := receiver.collectData(ctx); err != nil {
					receiver.logger.Error(fmt.Sprintf("Error occurred during data collection %v", err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (receiver *googleCloudSpannerReceiver) Shutdown(context.Context) error {
	for _, projectReader := range receiver.projectReaders {
		projectReader.Shutdown()
	}

	receiver.cancel()

	return nil
}

func (receiver *googleCloudSpannerReceiver) initializeProjectReaders(ctx context.Context) error {
	receiver.projectReaders = make([]*statsreader.ProjectReader, len(receiver.config.Projects))

	readerConfig := statsreader.ReaderConfig{
		// TODO Add real configuration parameter setter
		BackFillEnabled:        false,
		TopMetricsQueryMaxRows: receiver.config.TopMetricsQueryMaxRows,
	}

	for i, project := range receiver.config.Projects {
		projectReader, err := newProjectMetricsReader(project, readerConfig, ctx, receiver.logger)

		if err != nil {
			return err
		}

		receiver.projectReaders[i] = projectReader
	}

	return nil
}

func newProjectMetricsReader(project Project, readerConfig statsreader.ReaderConfig, ctx context.Context,
	logger *zap.Logger) (*statsreader.ProjectReader, error) {
	logger.Info(fmt.Sprintf("Constructing project reader for project id %v", project.ID))

	var databaseReaders []*statsreader.DatabaseReader

	for _, instance := range project.Instances {
		for _, database := range instance.Databases {
			logger.Info(fmt.Sprintf("Constructing database reader for project id %v, instance id %v, database %v",
				project.ID, instance.ID, database))

			databaseId := datasource.NewDatabaseId(project.ID, instance.ID, database)

			databaseReader, err := statsreader.NewDatabaseReader(ctx, databaseId,
				project.ServiceAccountKey, readerConfig, logger)

			if err != nil {
				return nil, err
			}

			databaseReaders = append(databaseReaders, databaseReader)
		}
	}

	return statsreader.NewProjectReader(databaseReaders, logger), nil
}

func (receiver *googleCloudSpannerReceiver) collectData(ctx context.Context) error {
	var allMetrics []pdata.Metrics

	for _, projectReader := range receiver.projectReaders {
		allMetrics = append(allMetrics, projectReader.Read(ctx)...)
	}

	for _, metric := range allMetrics {
		if err := receiver.nextConsumer.ConsumeMetrics(ctx, metric); err != nil {
			receiver.logger.Error(fmt.Sprintf("Failed to consume metric(s): %v because of an error %v",
				metricName(metric), err))
			return err
		}
	}

	return nil
}

func metricName(metric pdata.Metrics) string {
	var mName string
	resourceMetrics := metric.ResourceMetrics()

	for i := 0; i < resourceMetrics.Len(); i++ {
		ilm := resourceMetrics.At(i).InstrumentationLibraryMetrics()

		for j := 0; j < ilm.Len(); j++ {
			metrics := ilm.At(j).Metrics()

			for k := 0; k < metrics.Len(); k++ {
				mName += metrics.At(k).Name() + ","
			}
		}
	}

	if mName != "" {
		mName = mName[:len(mName)-1]
	}

	return mName
}
