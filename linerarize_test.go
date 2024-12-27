package linearize

import (
	"testing"

	"github.com/fgrzl/linearize/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item3", "item4"},
		}

		linearized1, err := Linearize(msg1)
		require.NoError(t, err)
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)
		_, _, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act: Merge the second linearized message into the first using the update mask
		merged, err := Merge(mask, linearized1, linearized2)

		// Assert: Verify that the merged result contains both the changes from msg2 and msg1
		assert.Equal(t, msg2.Field1, merged[1])
		assert.Equal(t, msg2.Field2, merged[2])
		assert.ElementsMatch(t, msg2.Repeated, merged[3])
	})

	t.Run("should merge messages using update mask when array grows", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"value1", "value2", "item3", "item4"},
		}

		linearized1, err := Linearize(msg1)
		require.NoError(t, err)
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)
		_, _, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act: Merge the second linearized message into the first using the update mask
		merged, err := Merge(mask, linearized1, linearized2)

		// Assert: Verify that the merged result contains both the changes from msg2 and msg1
		assert.Equal(t, msg2.Field1, merged[1])
		assert.Equal(t, msg2.Field2, merged[2])
		assert.ElementsMatch(t, msg2.Repeated, merged[3])
	})

	t.Run("should merge messages using update mask when array shrinks", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item1"},
		}

		linearized1, err := Linearize(msg1)
		require.NoError(t, err)
		linearized2, err := Linearize(msg2)
		require.NoError(t, err)
		_, _, mask, err := Diff(linearized1, linearized2)
		require.NoError(t, err)

		// Act: Merge the second linearized message into the first using the update mask
		merged, err := Merge(mask, linearized1, linearized2)

		// Assert: Verify that the merged result contains both the changes from msg2 and msg1
		assert.Equal(t, msg2.Field1, merged[1])
		assert.Equal(t, msg2.Field2, merged[2])
		assert.ElementsMatch(t, msg2.Repeated, merged[3])
	})
}

func TestComplex(t *testing.T) {
	t.Run("Linearize Complex Message", func(t *testing.T) {
		// Arrange: Create and linearize a complex message
		msg := mocks.CreateComplexMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act: Unlinearize back to the original complex message type
		var unlinearized mocks.Complex
		err = Unlinearize(linearized, &unlinearized)

		// Assert: Verify the unlinearized message matches the original
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.Equal(t, msg.Nested.Field1, unlinearized.Nested.Field1)
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	t.Run("Unlinearize Empty Complex Data", func(t *testing.T) {
		// Arrange
		var emptyLinearized LinearizedObject

		// Act: Attempt to unlinearize empty data
		var unlinearized mocks.Complex
		err := Unlinearize(emptyLinearized, &unlinearized)

		// Assert: Ensure an error is returned due to the empty data
		assert.Error(t, err)
	})

	// Diff and Merge Tests for Complex
	t.Run("Diff Complex Messages with Differences", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateComplexMessage()
		msg2 := &mocks.Complex{
			Field1: "updated_field1", // Modify some fields
			Field2: 200,
			Nested: &mocks.Simple{
				Field1: "nested_changed_field1",
			},
		}

		linearized1, err := Linearize(msg1)
		assert.NoError(t, err)
		linearized2, err := Linearize(msg2)
		assert.NoError(t, err)

		// Act: Diff the two linearized messages
		before, after, mask, err := Diff(linearized1, linearized2)

		// Assert: Ensure the differences are correctly identified and the mask is populated
		assert.NoError(t, err)
		assert.NotEmpty(t, before)
		assert.NotEmpty(t, after)
		assert.NotEmpty(t, mask)

		// // Verify that the mask contains the fields that have changed
		// assert.Contains(t, mask, &UpdateMask{
		// 	Value: &UpdateMask_Single{Single: 1}, // Field1 has changed (using field number 1)
		// })
		// assert.Contains(t, mask, &UpdateMask{
		// 	Value: &UpdateMask_Single{Single: 2}, // Nested.Field1 has changed (using nested field number)
		// })
	})

	t.Run("Merge Two Complex Messages with Update Mask", func(t *testing.T) {
		// Arrange: Create two different complex messages
		msg1 := mocks.CreateComplexMessage()
		msg2 := &mocks.Complex{
			Field1: "changed_field1", // Modify some fields
			Field2: 200,
			Repeated: []*mocks.Simple{
				{Field1: "item3", Field2: 3},
			},
		}

		linearized1, err := Linearize(msg1)
		assert.NoError(t, err)
		linearized2, err := Linearize(msg2)
		assert.NoError(t, err)

		// Act: Diff to get the update mask
		_, _, mask, err := Diff(linearized1, linearized2)
		assert.NoError(t, err)

		// Act: Merge the second linearized message into the first using the update mask
		merged, err := Merge(mask, linearized1, linearized2)

		// Assert: Verify that the merged result combines the changes
		assert.Equal(t, msg2.Field1, merged[1])
		assert.Equal(t, msg2.Field2, merged[2])
		assert.ElementsMatch(t, msg2.Repeated, merged[3])
	})
}
