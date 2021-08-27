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
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

type Config struct {
	scraperhelper.ScraperControllerSettings `mapstructure:",squash"`

	Projects []Project `mapstructure:"projects"`
}

type Project struct {
	ID                string     `mapstructure:"project_id"`
	ServiceAccountKey string     `mapstructure:"service_account_key"`
	Instances         []Instance `mapstructure:"instances"`
}

type Instance struct {
	ID        string     `mapstructure:"id"`
	Name      string     `mapstructure:"name"`
	Databases []Database `mapstructure:"databases"`
}

type Database struct {
	Name string `mapstructure:"name"`
}

func (config *Config) Validate() error {
	if config.CollectionInterval <= 0 {
		return fmt.Errorf("%v `interval` must be positive: %vms", config.ID(), config.CollectionInterval.Milliseconds())
	}

	if len(config.Projects) <= 0 {
		return fmt.Errorf("%v missing required field %v or its value is empty", config.ID(), "`projects`")
	}

	for _, project := range config.Projects {
		if err := project.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (project Project) Validate() error {
	var missingFields []string

	if project.ID == "" {
		missingFields = append(missingFields, "`project_id`")
	}

	if project.ServiceAccountKey == "" {
		missingFields = append(missingFields, "`service_account_key`")
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("field(s) %v is(are) required for project configuration", strings.Join(missingFields, ", "))
	}

	if len(project.Instances) <= 0 {
		return fmt.Errorf("field %v is required and cannot be empty for project configuration", "`instances`")
	}

	for _, instance := range project.Instances {
		if err := instance.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (instance Instance) Validate() error {
	var missingFields []string

	if instance.ID == "" {
		missingFields = append(missingFields, "`id`")
	}

	if instance.Name == "" {
		missingFields = append(missingFields, "`name`")
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("field(s) %v is(are) required for instance configuration", strings.Join(missingFields, ", "))
	}

	if len(instance.Databases) <= 0 {
		return fmt.Errorf("field %v is required and cannot be empty for instance configuration", "`databases`")
	}

	for _, database := range instance.Databases {
		if err := database.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (database Database) Validate() error {
	if database.Name == "" {
		return fmt.Errorf("field %v is required for database configuration", "`name`")
	}

	return nil
}
