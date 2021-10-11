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
	"fmt"
	"go.opentelemetry.io/collector/config"
)

type Config struct {
	config.ProcessorSettings `mapstructure:",squash"`

	TimeSeriesQuota int      `mapstructure:"time_series_quota"`
}

var _ config.Processor = (*Config)(nil)

func (config *Config) Validate() error {
	if config.TimeSeriesQuota > 200_000 {
		return fmt.Errorf("%v `time_series_quota` must be not greater than 200 000, current value is %vs",
			config.ID(), config.TimeSeriesQuota)
	}

	return nil
}