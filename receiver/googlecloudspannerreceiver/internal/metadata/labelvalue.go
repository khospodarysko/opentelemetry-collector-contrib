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

package metadata

import (
	"sort"
	"strings"
)

type LabelValueMetadata interface {
	GetLabelName() string
	GetLabelColumnName() string
	ValueHolder() interface{}
}

type LabelValue interface {
	LabelValueMetadata
	GetValue() interface{}
}

type QueryLabelValueMetadata struct {
	LabelName       string
	LabelColumnName string
}

type StringLabelValueMetadata struct {
	QueryLabelValueMetadata
}

type Int64LabelValueMetadata struct {
	QueryLabelValueMetadata
}

type BoolLabelValueMetadata struct {
	QueryLabelValueMetadata
}

type StringSliceLabelValueMetadata struct {
	QueryLabelValueMetadata
}

type ByteSliceLabelValueMetadata struct {
	QueryLabelValueMetadata
}

type StringLabelValue struct {
	StringLabelValueMetadata
	Value string
}

type Int64LabelValue struct {
	Int64LabelValueMetadata
	Value int64
}

type BoolLabelValue struct {
	BoolLabelValueMetadata
	Value bool
}

type StringSliceLabelValue struct {
	StringSliceLabelValueMetadata
	Value string
}

type ByteSliceLabelValue struct {
	ByteSliceLabelValueMetadata
	Value string
}

func (metadata QueryLabelValueMetadata) GetLabelName() string {
	return metadata.LabelName
}

func (metadata QueryLabelValueMetadata) GetLabelColumnName() string {
	return metadata.LabelColumnName
}

func (metadata StringLabelValueMetadata) ValueHolder() interface{} {
	var valueHolder string

	return &valueHolder
}

func (value StringLabelValue) GetValue() interface{} {
	return value.Value
}

func NewStringLabelValue(metadata StringLabelValueMetadata, valueHolder interface{}) StringLabelValue {
	return StringLabelValue{
		StringLabelValueMetadata: metadata,
		Value:                    *valueHolder.(*string),
	}
}

func (metadata Int64LabelValueMetadata) ValueHolder() interface{} {
	var valueHolder int64

	return &valueHolder
}

func (value Int64LabelValue) GetValue() interface{} {
	return value.Value
}

func NewInt64LabelValue(metadata Int64LabelValueMetadata, valueHolder interface{}) Int64LabelValue {
	return Int64LabelValue{
		Int64LabelValueMetadata: metadata,
		Value:                   *valueHolder.(*int64),
	}
}

func (metadata BoolLabelValueMetadata) ValueHolder() interface{} {
	var valueHolder bool

	return &valueHolder
}

func (value BoolLabelValue) GetValue() interface{} {
	return value.Value
}

func NewBoolLabelValue(metadata BoolLabelValueMetadata, valueHolder interface{}) BoolLabelValue {
	return BoolLabelValue{
		BoolLabelValueMetadata: metadata,
		Value:                  *valueHolder.(*bool),
	}
}

func (metadata StringSliceLabelValueMetadata) ValueHolder() interface{} {
	var valueHolder []string

	return &valueHolder
}

func (value StringSliceLabelValue) GetValue() interface{} {
	return value.Value
}

func NewStringSliceLabelValue(metadata StringSliceLabelValueMetadata, valueHolder interface{}) StringSliceLabelValue {
	value := *valueHolder.(*[]string)

	sort.Strings(value)

	sortedAndConstructedValue := strings.Join(value, ",")

	return StringSliceLabelValue{
		StringSliceLabelValueMetadata: metadata,
		Value:                         sortedAndConstructedValue,
	}
}

func (metadata ByteSliceLabelValueMetadata) ValueHolder() interface{} {
	var valueHolder []byte

	return &valueHolder
}

func (value ByteSliceLabelValue) GetValue() interface{} {
	return value.Value
}

func NewByteSliceLabelValue(metadata ByteSliceLabelValueMetadata, valueHolder interface{}) ByteSliceLabelValue {
	return ByteSliceLabelValue{
		ByteSliceLabelValueMetadata: metadata,
		Value:                       string(*valueHolder.(*[]byte)),
	}
}
