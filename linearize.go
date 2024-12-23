package linearize

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

type LinearizedData []interface{}

func Linearize(protoMsg proto.Message) (LinearizedData, error) {
	visited := make(map[uintptr]struct{})
	return linearizeWithReflection(protoMsg, visited)
}

func linearizeWithReflection(protoMsg proto.Message, visited map[uintptr]struct{}) (LinearizedData, error) {
	val := reflect.ValueOf(protoMsg)
	if val.Kind() == reflect.Ptr {
		ptr := val.Pointer()
		if _, exists := visited[ptr]; exists {
			return nil, fmt.Errorf("cyclic reference detected at pointer %d", ptr)
		}
		visited[ptr] = struct{}{}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct, got %s", val.Type().String())
	}

	var result LinearizedData
	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		fieldValue := val.Field(i)

		if !field.IsExported() || !fieldValue.IsValid() {
			continue
		}

		switch fieldValue.Kind() {
		case reflect.Ptr:
			if fieldValue.IsNil() {
				result = append(result, nil)
				continue
			}
			fallthrough // If not nil, handle as a struct

		case reflect.Struct:
			nestedProtoMsg, ok := fieldValue.Interface().(proto.Message)
			if ok {
				nestedData, err := linearizeWithReflection(nestedProtoMsg, visited)
				if err != nil {
					return nil, err
				}
				result = append(result, nestedData)
			} else {
				result = append(result, fieldValue.Interface())
			}
		case reflect.Slice:
			if fieldValue.Len() == 0 {
				result = append(result, nil)
				continue
			}
			repeatedItems, err := handleRepeatedField(fieldValue, visited)
			if err != nil {
				return nil, err
			}
			result = append(result, repeatedItems)
		case reflect.Map:
			mapResult := make(map[string]interface{})
			if fieldValue.Len() == 0 {
				result = append(result, nil) // Append nil for empty maps
				continue
			}

			for _, key := range fieldValue.MapKeys() {
				mapKey := fmt.Sprintf("%v", key.Interface())
				mapValue := fieldValue.MapIndex(key)

				if mapValue.Kind() == reflect.Ptr && mapValue.IsNil() {
					mapResult[mapKey] = nil
					continue
				}

				nestedProtoMsg, ok := mapValue.Interface().(proto.Message)
				if ok {
					nestedData, err := linearizeWithReflection(nestedProtoMsg, visited)
					if err != nil {
						return nil, err
					}
					mapResult[mapKey] = nestedData
				} else {
					mapResult[mapKey] = mapValue.Interface()
				}
			}
			result = append(result, mapResult)

		default:
			result = append(result, fieldValue.Interface())
		}
	}
	return result, nil
}

func handleRepeatedField(fieldValue reflect.Value, visited map[uintptr]struct{}) ([]interface{}, error) {
	var repeatedItems []interface{}
	for j := 0; j < fieldValue.Len(); j++ {
		item := fieldValue.Index(j)
		nestedProtoMsg, ok := item.Interface().(proto.Message)
		if ok {
			nestedData, err := linearizeWithReflection(nestedProtoMsg, visited)
			if err != nil {
				return nil, err
			}
			repeatedItems = append(repeatedItems, nestedData)
		} else {
			repeatedItems = append(repeatedItems, item.Interface())
		}
	}
	return repeatedItems, nil
}

func Diff(previous, latest LinearizedData) (before LinearizedData, after LinearizedData, mask []*UpdateMask) {
	// Ensure previous and latest are of the same length
	if len(previous) != len(latest) {
		panic(fmt.Sprintf("Mismatched lengths: previous (%d) and latest (%d)", len(previous), len(latest)))
	}

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
				// Ensure the types match before calling Diff
				previousSlice, ok := previous[i].([]interface{})
				latestSlice, okLatest := latest[i].([]interface{})
				if ok && okLatest {
					// Recursively compare nested arrays or slices
					nestedBefore, nestedAfter, nestedMask := Diff(previousSlice, latestSlice)
					if len(nestedMask) > 0 {
						// If there's a difference in the nested array, update the diff and mask
						before[i] = nestedBefore
						after[i] = nestedAfter
						mask = append(mask, nestedMask...)
					}
				} else {
					// Handle case where type assertion fails
					fmt.Printf("Type mismatch at index %d: expected []interface{}, got previous: %T, latest: %T\n", i, previous[i], latest[i])
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
