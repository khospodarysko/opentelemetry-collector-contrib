// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlecloudspannerreceiver

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configcheck"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.Nil(t, err)

	factory := NewFactory()
	factories.Receivers[typeStr] = factory
	cfg, err := configtest.LoadConfigAndValidate(path.Join(".", "testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Receivers), 1)

	receiver := cfg.Receivers[config.NewID(typeStr)].(*Config)
	require.NoError(t, configcheck.ValidateConfig(receiver))

	assert.Equal(t, len(receiver.Projects), 2)

	assert.Equal(t, receiver.Projects[0].ID, "spanner project 1")
	assert.Equal(t, receiver.Projects[0].ServiceAccountKey, "path to spanner project 1 service account json key")
	assert.Equal(t, len(receiver.Projects[0].Instances), 2)

	assert.Equal(t, receiver.Projects[0].Instances[0].ID, "id1")
	assert.Equal(t, receiver.Projects[0].Instances[0].Name, "First instance")
	assert.Equal(t, len(receiver.Projects[0].Instances[0].Databases), 2)
	assert.Equal(t, receiver.Projects[0].Instances[0].Databases[0].Name, "db11")
	assert.Equal(t, receiver.Projects[0].Instances[0].Databases[1].Name, "db12")
	assert.Equal(t, receiver.Projects[0].Instances[1].ID, "id2")
	assert.Equal(t, receiver.Projects[0].Instances[1].Name, "Second instance")
	assert.Equal(t, len(receiver.Projects[0].Instances[1].Databases), 2)
	assert.Equal(t, receiver.Projects[0].Instances[1].Databases[0].Name, "db21")
	assert.Equal(t, receiver.Projects[0].Instances[1].Databases[1].Name, "db22")

	assert.Equal(t, receiver.Projects[1].ID, "spanner project 2")
	assert.Equal(t, receiver.Projects[1].ServiceAccountKey, "path to spanner project 2 service account json key")
	assert.Equal(t, len(receiver.Projects[1].Instances), 2)

	assert.Equal(t, receiver.Projects[1].Instances[0].ID, "id3")
	assert.Equal(t, receiver.Projects[1].Instances[0].Name, "Third instance")
	assert.Equal(t, len(receiver.Projects[1].Instances[0].Databases), 2)
	assert.Equal(t, receiver.Projects[1].Instances[0].Databases[0].Name, "db31")
	assert.Equal(t, receiver.Projects[1].Instances[0].Databases[1].Name, "db32")
	assert.Equal(t, receiver.Projects[1].Instances[1].ID, "id4")
	assert.Equal(t, receiver.Projects[1].Instances[1].Name, "Fourth instance")
	assert.Equal(t, len(receiver.Projects[1].Instances[1].Databases), 2)
	assert.Equal(t, receiver.Projects[1].Instances[1].Databases[0].Name, "db41")
	assert.Equal(t, receiver.Projects[1].Instances[1].Databases[1].Name, "db42")
}

func TestValidateDatabase(t *testing.T) {
	err := Database{Name: "name"}.Validate()

	require.NoError(t, err)

	err = Database{Name: ""}.Validate()

	require.Error(t, err)
}

func TestValidateInstance(t *testing.T) {
	// All required fields are populated
	database := Database{Name: "name"}

	instance := Instance{
		ID: "id",
		Name: "name",
		Databases: []Database{database},
	}

	err := instance.Validate()

	require.NoError(t, err)

	// No id
	instance = Instance{
		ID: "",
		Name: "name",
		Databases: []Database{database},
	}

	err = instance.Validate()

	require.Error(t, err)

	// No name
	instance = Instance{
		ID: "id",
		Name: "",
		Databases: []Database{database},
	}

	err = instance.Validate()

	require.Error(t, err)

	// No databases
	instance = Instance{
		ID: "id",
		Name: "name",
	}

	err = instance.Validate()

	require.Error(t, err)
}

func TestValidateProject(t *testing.T) {
	// All required fields are populated
	database := Database{Name: "name"}

	instance := Instance{
		ID:        "id",
		Name:      "name",
		Databases: []Database{database},
	}

	project := Project{
		ID:                "id",
		ServiceAccountKey: "key",
		Instances:         []Instance{instance},
	}

	err := project.Validate()

	require.NoError(t, err)

	// No id
	project = Project{
		ID:                "",
		ServiceAccountKey: "key",
		Instances:         []Instance{instance},
	}

	err = project.Validate()

	require.Error(t, err)

	// No service account key
	project = Project{
		ID:                "id",
		ServiceAccountKey: "",
		Instances:         []Instance{instance},
	}

	err = project.Validate()

	require.Error(t, err)

	// No instances
	project = Project{
		ID:                "id",
		ServiceAccountKey: "key",
	}

	err = project.Validate()

	require.Error(t, err)
}

func TestValidateConfig(t *testing.T) {
	// All required fields are populated
	database := Database{Name: "name"}

	instance := Instance{
		ID:        "id",
		Name:      "name",
		Databases: []Database{database},
	}

	project := Project{
		ID:                "id",
		ServiceAccountKey: "key",
		Instances:         []Instance{instance},
	}

	cfg := &Config{
		ScraperControllerSettings: scraperhelper.ScraperControllerSettings{
			ReceiverSettings:   config.NewReceiverSettings(config.NewID(typeStr)),
			CollectionInterval: defaultCollectionInterval,
		},
		Projects: []Project{project},
	}

	err := cfg.Validate()

	require.NoError(t, err)

	// Invalid collection interval
	cfg = &Config{
		ScraperControllerSettings: scraperhelper.ScraperControllerSettings{
			ReceiverSettings:   config.NewReceiverSettings(config.NewID(typeStr)),
			CollectionInterval: -1,
		},
		Projects: []Project{project},
	}

	err = cfg.Validate()

	require.Error(t, err)

	// No projects
	cfg = &Config{
		ScraperControllerSettings: scraperhelper.ScraperControllerSettings{
			ReceiverSettings:   config.NewReceiverSettings(config.NewID(typeStr)),
			CollectionInterval: defaultCollectionInterval,
		},
	}

	err = cfg.Validate()

	require.Error(t, err)
}
