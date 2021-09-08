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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringLabelValueMetadata(t *testing.T) {
	metadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.GetLabelName())
	assert.Equal(t, labelColumnName, metadata.GetLabelColumnName())

	var expectedType *string

	assert.IsType(t, expectedType, metadata.ValueHolder())
}

func TestInt64LabelValueMetadata(t *testing.T) {
	metadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.GetLabelName())
	assert.Equal(t, labelColumnName, metadata.GetLabelColumnName())

	var expectedType *int64

	assert.IsType(t, expectedType, metadata.ValueHolder())
}

func TestBoolLabelValueMetadata(t *testing.T) {
	metadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.GetLabelName())
	assert.Equal(t, labelColumnName, metadata.GetLabelColumnName())

	var expectedType *bool

	assert.IsType(t, expectedType, metadata.ValueHolder())
}

func TestStringSliceLabelValueMetadata(t *testing.T) {
	metadata := StringSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.GetLabelName())
	assert.Equal(t, labelColumnName, metadata.GetLabelColumnName())

	var expectedType *[]string

	assert.IsType(t, expectedType, metadata.ValueHolder())
}

func TestByteSliceLabelValueMetadata(t *testing.T) {
	metadata := ByteSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	assert.Equal(t, labelName, metadata.GetLabelName())
	assert.Equal(t, labelColumnName, metadata.GetLabelColumnName())

	var expectedType *[]byte

	assert.IsType(t, expectedType, metadata.ValueHolder())
}

func TestStringLabelValue(t *testing.T) {
	metadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	labelValue :=
		StringLabelValue{
			StringLabelValueMetadata: metadata,
			Value:                    stringValue,
		}

	assert.Equal(t, metadata, labelValue.StringLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.GetValue())
}

func TestInt64LabelValue(t *testing.T) {
	metadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	labelValue :=
		Int64LabelValue{
			Int64LabelValueMetadata: metadata,
			Value:                   int64Value,
		}

	assert.Equal(t, metadata, labelValue.Int64LabelValueMetadata)
	assert.Equal(t, int64Value, labelValue.GetValue())
}

func TestBoolLabelValue(t *testing.T) {
	metadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	labelValue :=
		BoolLabelValue{
			BoolLabelValueMetadata: metadata,
			Value:                  boolValue,
		}

	assert.Equal(t, metadata, labelValue.BoolLabelValueMetadata)
	assert.Equal(t, boolValue, labelValue.GetValue())
}

func TestStringSliceLabelValue(t *testing.T) {
	metadata := StringSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	labelValue :=
		StringSliceLabelValue{
			StringSliceLabelValueMetadata: metadata,
			Value:                         stringValue,
		}

	assert.Equal(t, metadata, labelValue.StringSliceLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.GetValue())
}

func TestByteSliceLabelValue(t *testing.T) {
	metadata := ByteSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	labelValue :=
		ByteSliceLabelValue{
			ByteSliceLabelValueMetadata: metadata,
			Value:                       stringValue,
		}

	assert.Equal(t, metadata, labelValue.ByteSliceLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.GetValue())
}

func TestNewStringLabelValue(t *testing.T) {
	metadata := StringLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	value := stringValue
	valueHolder := &value

	labelValue := NewStringLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.StringLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.GetValue())
}

func TestNewInt64LabelValue(t *testing.T) {
	metadata := Int64LabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	value := int64Value
	valueHolder := &value

	labelValue := NewInt64LabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.Int64LabelValueMetadata)
	assert.Equal(t, int64Value, labelValue.GetValue())
}

func TestNewBoolLabelValue(t *testing.T) {
	metadata := BoolLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	value := boolValue
	valueHolder := &value

	labelValue := NewBoolLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.BoolLabelValueMetadata)
	assert.Equal(t, boolValue, labelValue.GetValue())
}

func TestNewStringSliceLabelValue(t *testing.T) {
	metadata := StringSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	value := []string{"b", "a", "c"}
	expectedValue := "a,b,c"
	valueHolder := &value

	labelValue := NewStringSliceLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.StringSliceLabelValueMetadata)
	assert.Equal(t, expectedValue, labelValue.GetValue())
}

func TestNewByteSliceLabelValue(t *testing.T) {
	metadata := ByteSliceLabelValueMetadata{
		QueryLabelValueMetadata{
			LabelName:       labelName,
			LabelColumnName: labelColumnName,
		},
	}

	value := []byte(stringValue)
	valueHolder := &value

	labelValue := NewByteSliceLabelValue(metadata, valueHolder)

	assert.Equal(t, metadata, labelValue.ByteSliceLabelValueMetadata)
	assert.Equal(t, stringValue, labelValue.GetValue())
}
