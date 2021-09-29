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

	"go.opentelemetry.io/collector/model/pdata"
)

type ReaderConfig struct {
	TopMetricsQueryMaxRows int
	BackfillEnabled        bool
}

type Reader interface {
	Name() string
	Read(ctx context.Context) ([]pdata.Metrics, error)
}

type CompositeReader interface {
	Name() string
	Read(ctx context.Context) []pdata.Metrics
	Shutdown()
}
