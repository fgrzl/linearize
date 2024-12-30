package linearize

import (
	"testing"

	"github.com/fgrzl/linearize/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSimple(t *testing.T) {
	t.Run("should linearize and unlinearize message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		require.NoError(t, err)

		// Act
		var unlinearized mocks.Simple
		err = Unlinearize(linearized, &unlinearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	t.Run("should return empty message given empty linearized object", func(t *testing.T) {
		// Arrange
		msg := &mocks.Simple{}
		var emptyLinearized LinearizedObject

		// Act: Attempt to unlinearize an empty structure
		var unlinearized mocks.Simple
		err := Unlinearize(emptyLinearized, &unlinearized)

		// Assert: Ensure an error is returned due to the empty data
		assert.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, int32(0), unlinearized.Field2) // Default value for missing Field2
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	t.Run("should unlinearize partial message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Remove a field to simulate a missing field (Field2 in this case)
		delete(linearized, 2)

		// Act
		var unlinearized mocks.Simple
		err = Unlinearize(linearized, &unlinearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, int32(0), unlinearized.Field2) // Default value for missing Field2
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	// Diff and Merge Tests
	t.Run("should diff messages with no changes", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act
		before, after, mask, err := Diff(linearized, linearized)

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, before)
		assert.Nil(t, after)
		assert.Nil(t, mask) // No updates expected
	})

	t.Run("should diff messages with changes", func(t *testing.T) {
		// Arrange
		msg1 := mocks.CreateSimpleMessage()
		linearized1, err := Linearize(msg1)
		require.NoError(t, err)

		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item3", "item4"},
		}
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)

		// Act
		before, after, mask, err := Diff(linearized1, linearized2)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, before)
		assert.NotNil(t, after)
		assert.NotNil(t, mask)
	})

	t.Run("should merge messages using update mask", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		linearized1, err := Linearize(msg1)
		require.NoError(t, err)

		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item3", "item4"},
		}
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)

		_, diff, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act
		err = Merge(mask, linearized1, diff)

		// Assert
		assert.Equal(t, msg2.Field1, linearized1[1])
		assert.Equal(t, msg2.Field2, linearized1[2])
		assert.Equal(t, msg2.Repeated[0], linearized1[3].(LinearizedSlice)[0])
		assert.Equal(t, msg2.Repeated[1], linearized1[3].(LinearizedSlice)[1])
	})

	t.Run("should merge messages using update mask when array grows", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		linearized1, err := Linearize(msg1)
		require.NoError(t, err)

		msg2 := &mocks.Simple{
			Field2:   200,
			Repeated: append(msg1.Repeated, "item3", "item4"),
		}
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)

		_, diff, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act
		err = Merge(mask, linearized1, diff)

		// Assert
		assert.Equal(t, nil, linearized1[1])
		assert.Equal(t, msg2.Field2, linearized1[2])
		assert.Equal(t, msg2.Repeated[0], linearized1[3].(LinearizedSlice)[0])
		assert.Equal(t, msg2.Repeated[1], linearized1[3].(LinearizedSlice)[1])
		assert.Equal(t, msg2.Repeated[2], linearized1[3].(LinearizedSlice)[2])
		assert.Equal(t, msg2.Repeated[3], linearized1[3].(LinearizedSlice)[3])
	})

	t.Run("should merge messages using update mask when array shrinks", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		linearized1, err := Linearize(msg1)
		require.NoError(t, err)

		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item3"},
		}
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)

		_, diff, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act
		err = Merge(mask, linearized1, diff)

		// Assert
		assert.Equal(t, msg2.Field1, linearized1[1])
		assert.Equal(t, msg2.Field2, linearized1[2])
		assert.Len(t, linearized1[3].(LinearizedSlice), 1)
		assert.Equal(t, msg2.Repeated[0], linearized1[3].(LinearizedSlice)[0])
	})
}
func TestComplex(t *testing.T) {
	t.Run("should linearize and unlinearize message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateComplexMessage()
		linearized, err := Linearize(msg)
		require.NoError(t, err)

		// Act
		var unlinearized mocks.Complex
		err = Unlinearize(linearized, &unlinearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)

		// Use proto.Equal for nested Protobuf message comparison
		assert.True(t, proto.Equal(msg.Nested, unlinearized.Nested), "Nested messages do not match")

		// Compare repeated fields
		require.Len(t, msg.Repeated, len(unlinearized.Repeated), "Repeated fields length mismatch")
		for i := range msg.Repeated {
			assert.True(t, proto.Equal(msg.Repeated[i], unlinearized.Repeated[i]), "Repeated field mismatch at index %d", i)
		}

		// Compare map fields
		require.Len(t, msg.Map, len(unlinearized.Map), "Map fields length mismatch")
		for key, val := range msg.Map {
			unlinearizedVal, ok := unlinearized.Map[key]
			assert.True(t, ok, "Key %v missing in unlinearized map", key)
			assert.True(t, proto.Equal(val, unlinearizedVal), "Map value mismatch for key %v", key)
		}
	})

	t.Run("should return empty message given empty linearized object", func(t *testing.T) {
		// Arrange
		msg := &mocks.Complex{}
		var emptyLinearized LinearizedObject

		// Act
		var unlinearized mocks.Complex
		err := Unlinearize(emptyLinearized, &unlinearized)

		// Assert
		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)

		// Use proto.Equal for nested Protobuf message comparison
		assert.True(t, proto.Equal(msg.Nested, unlinearized.Nested), "Nested messages do not match")

		// Compare repeated fields
		require.Len(t, msg.Repeated, len(unlinearized.Repeated), "Repeated fields length mismatch")
		for i := range msg.Repeated {
			assert.True(t, proto.Equal(msg.Repeated[i], unlinearized.Repeated[i]), "Repeated field mismatch at index %d", i)
		}

		// Compare map fields
		require.Len(t, msg.Map, len(unlinearized.Map), "Map fields length mismatch")
		for key, val := range msg.Map {
			unlinearizedVal, ok := unlinearized.Map[key]
			assert.True(t, ok, "Key %v missing in unlinearized map", key)
			assert.True(t, proto.Equal(val, unlinearizedVal), "Map value mismatch for key %v", key)
		}
	})

	t.Run("should unlinearize partial message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateComplexMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Remove a field to simulate a missing field
		delete(linearized, 2)

		// Act
		var unlinearized mocks.Complex
		err = Unlinearize(linearized, &unlinearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, int32(0), unlinearized.Field2)

		// Use proto.Equal for nested Protobuf message comparison
		assert.True(t, proto.Equal(msg.Nested, unlinearized.Nested), "Nested messages do not match")

		// Compare repeated fields
		require.Len(t, msg.Repeated, len(unlinearized.Repeated), "Repeated fields length mismatch")
		for i := range msg.Repeated {
			assert.True(t, proto.Equal(msg.Repeated[i], unlinearized.Repeated[i]), "Repeated field mismatch at index %d", i)
		}

		// Compare map fields
		require.Len(t, msg.Map, len(unlinearized.Map), "Map fields length mismatch")
		for key, val := range msg.Map {
			unlinearizedVal, ok := unlinearized.Map[key]
			assert.True(t, ok, "Key %v missing in unlinearized map", key)
			assert.True(t, proto.Equal(val, unlinearizedVal), "Map value mismatch for key %v", key)
		}
	})

	// Add Diff and Merge tests following the same pattern as Simple
	t.Run("should diff messages with changes", func(t *testing.T) {
		// Arrange
		msg1 := mocks.CreateComplexMessage()
		linearized1, err := Linearize(msg1)
		require.NoError(t, err)

		msg2 := &mocks.Complex{
			Field1:   "changed_field1",
			Field2:   200,
			Nested:   &mocks.Simple{Field1: "new_nested"},
			Repeated: []*mocks.Simple{{Field1: "new_repeated"}},
			Map:      map[string]*mocks.Simple{"key": {Field1: "new_map_value"}},
		}
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)

		// Act
		before, after, mask, err := Diff(linearized1, linearized2)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, before)
		assert.NotNil(t, after)
		assert.NotNil(t, mask)
	})

	t.Run("should merge messages using update mask", func(t *testing.T) {
		// Arrange
		msg1 := mocks.CreateComplexMessage()
		linearized1, err := Linearize(msg1)
		require.NoError(t, err)

		msg2 := &mocks.Complex{
			Field1:   "changed_field1",
			Field2:   200,
			Nested:   &mocks.Simple{Field1: "new_nested"},
			Repeated: []*mocks.Simple{{Field1: "new_repeated"}},
			Map:      map[string]*mocks.Simple{"key": {Field1: "new_map_value"}},
		}
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)

		_, diff, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act
		err = Merge(mask, linearized1, diff)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg2.Field1, linearized1[int32(1)])
		assert.Equal(t, msg2.Field2, linearized1[int32(2)])

	})
}
