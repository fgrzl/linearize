package linearize

import "fmt"

// Merge applies the UpdateMask operations (ADD, UPDATE, REMOVE) to the current LinearizedObject
// using the diff and the UpdateMask to create the updated object.
func Merge(mask *UpdateMask, current LinearizedObject, diff LinearizedObject) (LinearizedObject, error) {
	merged := make(LinearizedObject)

	// Iterate over all keys in the current LinearizedObject and apply the diff
	for key, value := range current {
		// Copy the current value into the merged result
		merged[key] = value
	}

	// Iterate over the mask and apply operations
	for pos, maskValue := range mask.Values {
		// Handle ADD operation
		if maskValue.Op == UpdateMaskOperation_ADD {
			// The key is new, add the corresponding value from the diff
			if diffVal, exists := diff[pos]; exists {
				merged[pos] = diffVal
			}
		}

		// Handle REMOVE operation
		if maskValue.Op == UpdateMaskOperation_REMOVE {
			// The key is removed, delete it from the merged object
			delete(merged, pos)
		}

		// Handle UPDATE operation
		if maskValue.Op == UpdateMaskOperation_UPDATE {
			// The key is updated, get the corresponding value from the diff
			if diffVal, exists := diff[pos]; exists {
				merged[pos] = diffVal
			}

			// If there's a nested mask, merge recursively
			if maskValue.Masks != nil {
				if nestedVal, exists := current[pos]; exists {
					// Handle nested structures: LinearizedObject, LinearizedSlice, LinearizedMap
					switch nestedVal := nestedVal.(type) {
					case LinearizedObject:
						// Recursively merge LinearizedObjects
						mergedValue, err := Merge(maskValue.Masks, nestedVal, diff[pos].(LinearizedObject))
						if err != nil {
							return nil, err
						}
						merged[pos] = mergedValue
					case LinearizedSlice:
						// Handle merging of LinearizedSlice (slices)
						mergedValue, err := mergeSlices(maskValue.Masks, nestedVal, diff[pos].(LinearizedSlice))
						if err != nil {
							return nil, err
						}
						merged[pos] = mergedValue
					case LinearizedMap:
						// Handle merging of LinearizedMap
						mergedValue, err := mergeMaps(maskValue.Masks, nestedVal, diff[pos].(LinearizedMap))
						if err != nil {
							return nil, err
						}
						merged[pos] = mergedValue
					default:
						return nil, fmt.Errorf("unsupported type %T for key %v", nestedVal, pos)
					}
				}
			}
		}
	}

	return merged, nil
}

// mergeSlices merges two LinearizedSlice types using the update mask
func mergeSlices(mask *UpdateMask, current, diff LinearizedSlice) (LinearizedSlice, error) {
	merged := make(LinearizedSlice)

	// Iterate over current and add the existing values to merged
	for pos, value := range current {
		merged[pos] = value
	}

	// Iterate over the mask and apply operations
	for pos, maskValue := range mask.Values {
		if maskValue.Op == UpdateMaskOperation_ADD {
			// Add the corresponding value from diff
			if diffVal, exists := diff[pos]; exists {
				merged[pos] = diffVal
			}
		}

		if maskValue.Op == UpdateMaskOperation_REMOVE {
			// Remove the value at the specified position
			delete(merged, pos)
		}

		if maskValue.Op == UpdateMaskOperation_UPDATE {
			// Update the value at the specified position
			if diffVal, exists := diff[pos]; exists {
				merged[pos] = diffVal
			}
		}
	}
	return merged, nil
}

// mergeMaps merges two LinearizedMap types using the update mask
func mergeMaps(mask *UpdateMask, current, diff LinearizedMap) (LinearizedMap, error) {
	merged := make(LinearizedMap)

	// Copy the current map to the merged map
	for key, value := range current {
		merged[key] = value
	}

	// Iterate over the mask and apply operations
	for pos, maskValue := range mask.Values {
		if maskValue.Op == UpdateMaskOperation_ADD {
			// Add the corresponding value from diff
			if diffVal, exists := diff[pos]; exists {
				merged[pos] = diffVal
			}
		}

		if maskValue.Op == UpdateMaskOperation_REMOVE {
			// Remove the key from the merged map
			delete(merged, pos)
		}

		if maskValue.Op == UpdateMaskOperation_UPDATE {
			// Update the value at the specified position
			if diffVal, exists := diff[pos]; exists {
				merged[pos] = diffVal
			}
		}
	}
	return merged, nil
}
