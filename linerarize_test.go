package linearize_test

import (
	"testing"

	"github.com/fgrzl/linearize"
	"github.com/fgrzl/linearize/mocks"
	"github.com/stretchr/testify/assert"
)

func TestLinearizeEdgeCases(t *testing.T) {
	t.Run("Simple message with all fields populated", func(t *testing.T) {
		message := &mocks.Simple{
			Field1:   "Test",
			Field2:   42,
			Repeated: []string{"one", "two", "three"},
		}
		expected := linearize.LinearizedData{
			"Test", int32(42),
			[]interface{}{"one", "two", "three"},
		}

		result, err := linearize.Linearize(message)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("Simple message with empty fields", func(t *testing.T) {
		message := &mocks.Simple{}
		expected := linearize.LinearizedData{"", int32(0), nil}

		result, err := linearize.Linearize(message)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("Complex message with nested and repeated fields", func(t *testing.T) {
		message := &mocks.Complex{
			Field1: "Parent",
			Field2: 99,
			Nested: &mocks.Simple{
				Field1: "Child",
				Field2: 42,
			},
			Repeated: []*mocks.Simple{
				{Field1: "Repeated1", Field2: 1},
				{Field1: "Repeated2", Field2: 2},
			},
		}
		expected := linearize.LinearizedData{
			"Parent", int32(99),
			linearize.LinearizedData{"Child", int32(42), nil},
			[]interface{}{
				linearize.LinearizedData{"Repeated1", int32(1), nil},
				linearize.LinearizedData{"Repeated2", int32(2), nil},
			},
			nil,
		}

		result, err := linearize.Linearize(message)
		assert.NoError(t, err)
		assert.EqualValues(t, expected, result)
	})

	t.Run("Complex message with map fields", func(t *testing.T) {
		message := &mocks.Complex{
			Map: map[string]*mocks.Simple{
				"key1": {Field1: "MapValue1", Field2: 11},
				"key2": {Field1: "MapValue2", Field2: 22},
			},
		}
		expected := linearize.LinearizedData{
			"", int32(0), nil, nil,
			map[string]interface{}{
				"key1": linearize.LinearizedData{"MapValue1", int32(11), nil},
				"key2": linearize.LinearizedData{"MapValue2", int32(22), nil},
			},
		}

		result, err := linearize.Linearize(message)
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
}

func TestDiffEdgeCases(t *testing.T) {
	t.Run("Detect changes in repeated fields", func(t *testing.T) {
		previous := linearize.LinearizedData{"Test", int32(42), []interface{}{"one", "two"}}
		latest := linearize.LinearizedData{"Test", int32(42), []interface{}{"one", "three"}}

		_, _, mask := linearize.Diff(previous, latest)

		assert.Len(t, mask, 1)
		assert.EqualValues(t, int32(2), mask[0].GetSingle())
	})

	t.Run("Detect changes in nested fields", func(t *testing.T) {
		previous := linearize.LinearizedData{
			"Parent", int32(99),
			linearize.LinearizedData{"Child", int32(42), nil},
			nil, nil,
		}
		latest := linearize.LinearizedData{
			"Parent", int32(99),
			linearize.LinearizedData{"Child", int32(43), nil},
			nil, nil,
		}

		_, _, mask := linearize.Diff(previous, latest)

		assert.Len(t, mask, 1)
		assert.Equal(t, int32(2), mask[0].GetSingle())
	})

	t.Run("Detect map field updates", func(t *testing.T) {
		previous := linearize.LinearizedData{
			"", int32(0), nil, nil,
			map[string]interface{}{
				"key1": linearize.LinearizedData{"MapValue1", int32(11), nil},
			},
		}
		latest := linearize.LinearizedData{
			"", int32(0), nil, nil,
			map[string]interface{}{
				"key1": linearize.LinearizedData{"UpdatedMapValue", int32(11), nil},
			},
		}

		_, _, mask := linearize.Diff(previous, latest)

		assert.Len(t, mask, 1)
		assert.Equal(t, int32(4), mask[0].GetSingle())
	})
}

func TestMergeEdgeCases(t *testing.T) {
	t.Run("Apply updates to repeated fields", func(t *testing.T) {
		updateMask := []*linearize.UpdateMask{
			{Value: &linearize.UpdateMask_Single{Single: 2}},
		}
		previous := linearize.LinearizedData{"Test", int32(42), []interface{}{"one", "two"}}
		latest := linearize.LinearizedData{"Test", int32(42), []interface{}{"one", "three"}}

		result := linearize.Merge(updateMask, previous, latest)

		assert.Equal(t, latest, result)
	})

	t.Run("Apply updates to map fields", func(t *testing.T) {
		updateMask := []*linearize.UpdateMask{
			{Value: &linearize.UpdateMask_Single{Single: 4}},
		}
		previous := linearize.LinearizedData{
			"", int32(0), nil, nil,
			map[string]interface{}{
				"key1": linearize.LinearizedData{"MapValue1", int32(11), nil},
			},
		}
		latest := linearize.LinearizedData{
			"", int32(0), nil, nil,
			map[string]interface{}{
				"key1": linearize.LinearizedData{"UpdatedMapValue", int32(11), nil},
			},
		}

		result := linearize.Merge(updateMask, previous, latest)

		assert.Equal(t, latest, result)
	})
}
