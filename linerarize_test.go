package linearize

import (
	"testing"

	"github.com/fgrzl/linearize/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	t.Run("Linearize Simple Message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)

		// Assert
		assert.NoError(t, err)

		// Act: Unlinearize the linearized data back to the original type
		var unlinearized mocks.Simple
		err = Unlinearize(linearized, &unlinearized)

		// Assert: Ensure the original and unlinearized data are the same
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	t.Run("Unlinearize Empty Linearized Data", func(t *testing.T) {
		// Arrange
		var emptyLinearized LinearizedObject

		// Act: Attempt to unlinearize an empty structure
		var unlinearized mocks.Simple
		err := Unlinearize(emptyLinearized, &unlinearized)

		// Assert: Ensure an error is returned due to the empty data
		assert.Error(t, err)
	})

	t.Run("Linearize and Unlinearize Missing Fields in Simple", func(t *testing.T) {
		// Arrange: Create message and linearize it
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Remove a field to simulate a missing field (Field2 in this case)
		delete(linearized, 2)

		// Act: Unlinearize and check if default values are used
		var unlinearized mocks.Simple
		err = Unlinearize(linearized, &unlinearized)

		// Assert: Ensure default values are applied for missing fields
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, int32(0), unlinearized.Field2) // Default value for missing Field2
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	t.Run("Linearize and Unlinearize Invalid Simple Data", func(t *testing.T) {
		// Arrange: Create invalid linearized data
		linearized := LinearizedObject{
			1: "test",
			2: 123,
		}

		// Act: Attempt to unlinearize invalid data
		var unlinearized mocks.Simple
		err := Unlinearize(linearized, &unlinearized)

		// Assert: Ensure an error occurs for invalid data structure
		assert.Error(t, err)
	})

	// Diff and Merge Tests
	t.Run("Diff Simple Message with No Changes", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act: Diff the linearized message with itself
		before, after, mask, err := Diff(linearized, linearized)

		// Assert: Ensure no differences are found
		assert.NoError(t, err)
		assert.Empty(t, before)
		assert.Empty(t, after)
		assert.Empty(t, mask) // No updates expected
	})

	t.Run("Diff Simple Message with Changes", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item3", "item4"},
		}

		linearized1, err := Linearize(msg1)
		assert.NoError(t, err)
		linearized2, err := Linearize(msg2)
		assert.NoError(t, err)

		// Act: Diff the two linearized messages
		before, after, mask, err := Diff(linearized1, linearized2)

		// Assert: Ensure differences are found and the update mask is correct
		assert.NoError(t, err)
		assert.NotEmpty(t, before)
		assert.NotEmpty(t, after)
		assert.NotEmpty(t, mask)

		// Verify that the mask contains the fields that have changed
		assert.Contains(t, mask, &UpdateMask{
			Value: &UpdateMask_Single{Single: 1}, // Field1 has changed (using field number 1)
		})
		assert.Contains(t, mask, &UpdateMask{
			Value: &UpdateMask_Single{Single: 2}, // Field2 has changed (using field number 2)
		})
		assert.Contains(t, mask, &UpdateMask{
			Value: &UpdateMask_Single{Single: 3}, // Repeated has changed (using field number 3)
		})
	})

	t.Run("Merge Simple Messages with Update Mask", func(t *testing.T) {
		// Arrange: Create two different messages
		msg1 := mocks.CreateSimpleMessage()
		msg2 := &mocks.Simple{
			Field1:   "changed_field1", // Modify some fields
			Field2:   200,
			Repeated: []string{"item3", "item4"},
		}

		linearized1, err := Linearize(msg1)
		assert.NoError(t, err)
		linearized2, err := Linearize(msg2)
		assert.NoError(t, err)

		// Act: Diff to get the update mask
		_, _, mask, err := Diff(linearized1, linearized2)
		assert.NoError(t, err)

		// Act: Merge the second linearized message into the first using the update mask
		merged := Merge(mask, linearized1, linearized2)

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

		// Verify that the mask contains the fields that have changed
		assert.Contains(t, mask, &UpdateMask{
			Value: &UpdateMask_Single{Single: 1}, // Field1 has changed (using field number 1)
		})
		assert.Contains(t, mask, &UpdateMask{
			Value: &UpdateMask_Single{Single: 2}, // Nested.Field1 has changed (using nested field number)
		})
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
		merged := Merge(mask, linearized1, linearized2)

		// Assert: Verify that the merged result combines the changes
		assert.Equal(t, msg2.Field1, merged[1])
		assert.Equal(t, msg2.Field2, merged[2])
		assert.ElementsMatch(t, msg2.Repeated, merged[3])
	})
}
