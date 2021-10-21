// Copyright  The OpenTelemetry Authors
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

package metadata

import (
	"fmt"
	"go.opentelemetry.io/collector/model/pdata"
	"sort"
	"strings"
)

type LabelValueMetadata interface {
	ValueMetadata
	NewLabelValue(value interface{}) LabelValue
}

type LabelValue interface {
	Metadata() LabelValueMetadata
	Value() interface{}
	SetAttributesTo(attributes pdata.AttributeMap)
}

type queryLabelValueMetadata struct {
	name              string
	columnName        string
	valueType         string
	newLabelValueFunc func(m queryLabelValueMetadata, value interface{}) LabelValue
	valueHolderFunc   func() interface{}
}

func (m queryLabelValueMetadata) ValueHolder() interface{} {
	return m.valueHolderFunc()
}

func (m queryLabelValueMetadata) NewLabelValue(value interface{}) LabelValue {
	return m.newLabelValueFunc(m, value)
}

type stringLabelValue struct {
	metadata queryLabelValueMetadata
	value    string
}

func (v stringLabelValue) Metadata() LabelValueMetadata {
	return v.metadata
}

type int64LabelValue struct {
	metadata queryLabelValueMetadata
	value    int64
}

func (v int64LabelValue) Metadata() LabelValueMetadata {
	return v.metadata
}

type boolLabelValue struct {
	metadata queryLabelValueMetadata
	value    bool
}

func (v boolLabelValue) Metadata() LabelValueMetadata {
	return v.metadata
}

type stringSliceLabelValue struct {
	metadata queryLabelValueMetadata
	value    string
}

func (v stringSliceLabelValue) Metadata() LabelValueMetadata {
	return v.metadata
}

type byteSliceLabelValue struct {
	metadata queryLabelValueMetadata
	value    string
}

func (v byteSliceLabelValue) Metadata() LabelValueMetadata {
	return v.metadata
}

func (m queryLabelValueMetadata) Name() string {
	return m.name
}

func (m queryLabelValueMetadata) ColumnName() string {
	return m.columnName
}

func (v stringLabelValue) Value() interface{} {
	return v.value
}

func newStringLabelValue(metadata queryLabelValueMetadata, valueHolder interface{}) LabelValue {
	return stringLabelValue{
		metadata: metadata,
		value:    *valueHolder.(*string),
	}
}

func (v int64LabelValue) Value() interface{} {
	return v.value
}

func newInt64LabelValue(metadata queryLabelValueMetadata, valueHolder interface{}) LabelValue {
	return int64LabelValue{
		metadata: metadata,
		value:    *valueHolder.(*int64),
	}
}

func (v boolLabelValue) Value() interface{} {
	return v.value
}

func newBoolLabelValue(metadata queryLabelValueMetadata, valueHolder interface{}) LabelValue {
	return boolLabelValue{
		metadata: metadata,
		value:    *valueHolder.(*bool),
	}
}

func (v stringSliceLabelValue) Value() interface{} {
	return v.value
}

func newStringSliceLabelValue(metadata queryLabelValueMetadata, valueHolder interface{}) LabelValue {
	value := *valueHolder.(*[]string)

	sort.Strings(value)

	sortedAndConstructedValue := strings.Join(value, ",")

	return stringSliceLabelValue{
		metadata: metadata,
		value:    sortedAndConstructedValue,
	}
}

func (v byteSliceLabelValue) Value() interface{} {
	return v.value
}

func newByteSliceLabelValue(metadata queryLabelValueMetadata, valueHolder interface{}) LabelValue {
	return byteSliceLabelValue{
		metadata: metadata,
		value:    string(*valueHolder.(*[]byte)),
	}
}

func (v stringLabelValue) SetAttributesTo(attributes pdata.AttributeMap) {
	attributes.InsertString(v.metadata.name, v.value)
}

func (v boolLabelValue) SetAttributesTo(attributes pdata.AttributeMap) {
	attributes.InsertBool(v.metadata.name, v.value)
}

func (v int64LabelValue) SetAttributesTo(attributes pdata.AttributeMap) {
	attributes.InsertInt(v.metadata.name, v.value)
}

func (v stringSliceLabelValue) SetAttributesTo(attributes pdata.AttributeMap) {
	attributes.InsertString(v.metadata.name, v.value)
}

func (v byteSliceLabelValue) SetAttributesTo(attributes pdata.AttributeMap) {
	attributes.InsertString(v.metadata.name, v.value)
}

func NewLabelValueMetadata(name string, columnName string, valueType string) (LabelValueMetadata, error) {
	var newLabelValueFunc func(m queryLabelValueMetadata, value interface{}) LabelValue
	var valueHolderFunc func() interface{}

	switch valueType {
	case "string":
		newLabelValueFunc = newStringLabelValue
		valueHolderFunc = func() interface{} {
			var valueHolder string
			return &valueHolder
		}
	case "int":
		newLabelValueFunc = newInt64LabelValue
		valueHolderFunc = func() interface{} {
			var valueHolder int64
			return &valueHolder
		}
	case "bool":
		newLabelValueFunc = newBoolLabelValue
		valueHolderFunc = func() interface{} {
			var valueHolder bool
			return &valueHolder
		}
	case "string_slice":
		newLabelValueFunc = newStringSliceLabelValue
		valueHolderFunc = func() interface{} {
			var valueHolder []string
			return &valueHolder
		}
	case "byte_slice":
		newLabelValueFunc = newByteSliceLabelValue
		valueHolderFunc = func() interface{} {
			var valueHolder []byte
			return &valueHolder
		}
	default:
		return nil, fmt.Errorf("invalid value type received for label %q", name)
	}

	return queryLabelValueMetadata{
		name:              name,
		columnName:        columnName,
		valueType:         valueType,
		newLabelValueFunc: newLabelValueFunc,
		valueHolderFunc:   valueHolderFunc,
	}, nil

}
