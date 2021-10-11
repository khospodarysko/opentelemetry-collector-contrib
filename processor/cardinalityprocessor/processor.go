// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cardinalityprocessor

import (
	"context"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
)

type cardinality struct {
	logger *zap.Logger
}

// ?
//var _ component.MetricsProcessor = (*cardinality)(nil)

func newCardinality(set component.ProcessorCreateSettings, cfg *Config) (*cardinality, error) {
	c := &cardinality{
		logger: set.Logger,
	}
	c.logger.Info("cardinality create")
	return c, nil
}

func (c *cardinality) Start(_ context.Context, host component.Host) error {
	c.logger.Info("cardinality start")
	return nil
}

func (c *cardinality) Shutdown(context.Context) error {
	c.logger.Info("cardinality shutdown")
	return nil
}

func (c *cardinality) processMetrics(ctx context.Context, md pdata.Metrics) (pdata.Metrics, error) {
	c.logger.Info("cardinality process metrics")
	return md, nil
}