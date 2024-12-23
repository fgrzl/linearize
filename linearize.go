package linearize

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

type LinearizedData []interface{}

func Linearize(protoMsg proto.Message) (LinearizedData, error) {
	val := reflect.ValueOf(protoMsg)
	if val.Kind() == reflect.Ptr {
		val = val.Elem() // Dereference pointer if it's a pointer
	}
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct, got %s", val.Kind())
	}

	var result LinearizedData

	// Iterate through the fields of the struct using reflection
	for i := 0; i < val.NumField(); i++ {
		fieldValue := val.Field(i)

		// Skip invalid fields (e.g., empty)
		if !fieldValue.IsValid() {
			result = append(result, nil)
			continue
		}

		// Check if the field is a nested message
		if fieldValue.Kind() == reflect.Struct {
			nestedArray, err := Linearize(fieldValue.Interface().(proto.Message))
			if err != nil {
				return nil, err
			}
			result = append(result, nestedArray)
		} else if fieldValue.Kind() == reflect.Slice {
			// Handle repeated fields (e.g., repeated IPAddress)
			if fieldValue.Len() > 0 {
				var repeatedItems []interface{}
				for j := 0; j < fieldValue.Len(); j++ {
					item := fieldValue.Index(j)
					if item.Kind() == reflect.Struct {
						nestedArray, err := Linearize(item.Interface().(proto.Message))
						if err != nil {
							return nil, err
						}
						repeatedItems = append(repeatedItems, nestedArray)
					} else {
						repeatedItems = append(repeatedItems, item.Interface())
					}
				}
				result = append(result, repeatedItems)
			} else {
				result = append(result, nil)
			}
		} else {
			// For simple types, just append the value to the result slice
			result = append(result, fieldValue.Interface())
		}
	}

	return result, nil
}

// Diff computes the difference between previous and the latest and produces a before and after and the update mask
func Diff(previous, latest LinearizedData) (before LinearizedData, after LinearizedData, mask []*UpdateMask) {
	// Initialize slices to store the before and after values
	before = make(LinearizedData, len(latest))
	after = make(LinearizedData, len(latest))

	// Loop through each element and compare it to the corresponding element in previous
	for i := range latest {
		// Copy previous and latest values to before and after slices
		before[i] = previous[i]
		after[i] = latest[i]

		// Check if the element is an array or slice
		if isArray(latest[i]) {
			// Check if the element is a slice and recurse if it's a slice of interfaces
			if previous[i] != nil && isArray(previous[i]) {
				// Recursively compare nested arrays or slices (and handle three return values)
				nestedBefore, nestedAfter, nestedMask := Diff(previous[i].([]interface{}), latest[i].([]interface{}))
				if len(nestedMask) > 0 {
					// If there's a difference in the nested array, update the diff and mask
					before[i] = nestedBefore
					after[i] = nestedAfter
					mask = append(mask, nestedMask...)
				}
			}
		} else {
			// Compare directly for primitive types or non-slice elements
			if previous[i] != latest[i] {
				// If there's a difference, update the diff and add a mask entry
				updateMask := &UpdateMask{
					Value: &UpdateMask_Single{
						Single: int32(i), // Using the index for identifying the update
					},
				}
				mask = append(mask, updateMask)
			}
		}
	}

	// Return before, after, and the mask
	return before, after, mask
}

// isArray checks if the value is a slice or array
func isArray(value interface{}) bool {
	v := reflect.ValueOf(value)
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

// Merge applies the updateMask to the right data and merges the changes from left into the right.
func Merge(updateMask []*UpdateMask, previous LinearizedData, latest LinearizedData) LinearizedData {
	merged := make(LinearizedData, len(latest))

	// Copy previous data into merged slice
	copy(merged, previous)

	// Iterate through the update masks and apply changes
	for _, mask := range updateMask {
		applyUpdate(merged, mask, latest)
	}

	return merged
}

// applyUpdate applies a single update from the UpdateMask to the merged data.
func applyUpdate(merged LinearizedData, mask *UpdateMask, latest LinearizedData) {
	if mask.GetSingle() != 0 {
		// Retrieve the index from the mask and update the merged data at that index
		index := int(mask.GetSingle())
		if index < len(merged) {
			merged[index] = latest[index]
		}
	} else if mask.GetMultiple() != nil {
		// Recursively apply the update for multiple values
		for i, nestedMask := range mask.GetMultiple().Values {
			applyUpdate(merged, nestedMask, latest[i].([]interface{}))
		}
	}
}
