package linearize

import (
	"testing"

	"github.com/fgrzl/linearize/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test for Linearize, Unlinearize, Diff, and Merge functions
func TestLinearize(t *testing.T) {

	// Test Linearize and Unlinearize Simple Message
	t.Run("Linearize and Unlinearize Simple Message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act
		unlinearized, err := Unlinearize[*mocks.Simple](linearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	// Test Linearize and Unlinearize Complex Message
	t.Run("Linearize and Unlinearize Complex Message", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateComplexMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act
		unlinearized, err := Unlinearize[*mocks.Complex](linearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.Equal(t, msg.Nested.Field1, unlinearized.Nested.Field1)
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
		assert.ElementsMatch(t, msg.Map["key1"].Repeated, unlinearized.Map["key1"].Repeated)
	})

	// Test Linearize and Unlinearize Empty Linearized Data
	t.Run("Linearize and Unlinearize Empty Linearized Data", func(t *testing.T) {
		// Arrange
		var emptyLinearized LinearizedObject

		// Act
		unlinearized, err := Unlinearize[*mocks.Simple](emptyLinearized)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, unlinearized)
	})

	// Test Linearize and Unlinearize Invalid Data Structure
	t.Run("Linearize and Unlinearize Invalid Data Structure", func(t *testing.T) {
		// Arrange
		linearized := LinearizedObject{
			1: "test",
			2: 123,
		}

		// Act
		unlinearized, err := Unlinearize[*mocks.Complex](linearized)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, unlinearized)
	})

	// Test Linearize and Unlinearize with Missing Fields
	t.Run("Linearize and Unlinearize with Missing Fields", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateSimpleMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Remove one field (simulating a missing field in LinearizedObject)
		delete(linearized, 2) // Remove Field2

		// Act
		unlinearized, err := Unlinearize[*mocks.Simple](linearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, int32(0), unlinearized.Field2) // Default value since Field2 was missing
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
	})

	// Test Linearize and Unlinearize with Nested Map
	t.Run("Linearize and Unlinearize with Nested Map", func(t *testing.T) {
		// Arrange
		msg := mocks.CreateComplexMessage()
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act
		unlinearized, err := Unlinearize[*mocks.Complex](linearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.Equal(t, msg.Nested.Field1, unlinearized.Nested.Field1)
		assert.ElementsMatch(t, msg.Repeated, unlinearized.Repeated)
		assert.ElementsMatch(t, msg.Map["key1"].Repeated, unlinearized.Map["key1"].Repeated)
	})

	// Test Linearize and Unlinearize with Empty Map
	t.Run("Linearize and Unlinearize with Empty Map", func(t *testing.T) {
		// Arrange
		msg := &mocks.Complex{
			Field1: "complex_field1",
			Field2: 100,
			Map:    map[string]*mocks.Simple{}, // Empty map
		}
		linearized, err := Linearize(msg)
		assert.NoError(t, err)

		// Act
		unlinearized, err := Unlinearize[*mocks.Complex](linearized)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, msg.Field1, unlinearized.Field1)
		assert.Equal(t, msg.Field2, unlinearized.Field2)
		assert.Len(t, unlinearized.Map, 0) // Empty map
	})

	// Test Diff - Detecting changes between two LinearizedObject
	t.Run("Diff Identifies Changes Between LinearizedObject", func(t *testing.T) {
		// Arrange
		previous := mocks.CreateComplexMessage()
		latest := mocks.CreateComplexMessage()

		// Modify some fields in latest
		latest.Field1 = "updated_field1"
		latest.Map["key1"].Field1 = "updated_key1"

		linearizedPrev, err := Linearize(previous)
		assert.NoError(t, err)

		linearizedLatest, err := Linearize(latest)
		assert.NoError(t, err)

		// Act
		_, after, mask, err := Diff(linearizedPrev, linearizedLatest)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "updated_field1", after[1]) // Updated Field1
		assert.Equal(t, "updated_key1", after[5])   // Updated Map["key1"]
		assert.Len(t, mask, 2)                      // Two changes should be present in the mask
	})

	// Test Merge - Merging changes into the latest LinearizedObject
	t.Run("Merge Applies Changes Correctly", func(t *testing.T) {
		// Arrange
		previous := mocks.CreateComplexMessage()
		latest := mocks.CreateComplexMessage()

		// Modify some fields in latest
		latest.Field1 = "updated_field1"
		latest.Map["key1"].Field1 = "updated_key1"

		linearizedPrev, err := Linearize(previous)
		assert.NoError(t, err)

		linearizedLatest, err := Linearize(latest)
		assert.NoError(t, err)

		_, _, mask, err := Diff(linearizedPrev, linearizedLatest)
		require.NoError(t, err)

		// Act
		merged := Merge(mask, linearizedPrev, linearizedLatest)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "updated_field1", merged[1]) // Updated Field1
		assert.Equal(t, "updated_key1", merged[5])   // Updated Map["key1"]
	})
}
