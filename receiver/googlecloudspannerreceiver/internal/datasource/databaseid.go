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

type DatabaseId struct {
	projectId    string
	instanceId   string
	databaseName string
	id           string
}

func NewDatabaseId(projectId string, instanceId string, databaseName string) *DatabaseId {
	return &DatabaseId{
		projectId:    projectId,
		instanceId:   instanceId,
		databaseName: databaseName,
		id:           "projects/" + projectId + "/instances/" + instanceId + "/databases/" + databaseName,
	}
}

func (databaseId *DatabaseId) ProjectId() string {
	return databaseId.projectId
}

func (databaseId *DatabaseId) InstanceId() string {
	return databaseId.instanceId
}

func (databaseId *DatabaseId) DatabaseName() string {
	return databaseId.databaseName
}
func (databaseId *DatabaseId) Id() string {
	return databaseId.id
}
