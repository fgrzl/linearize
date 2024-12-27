package linearize

import "fmt"

// LinearizedObject, LinearizedMap, and LinearizedArray are assumed to be types that have been defined elsewhere
// UpdateMask is assumed to have a field `Values` which maps keys to UpdateMaskValue objects

// Merge applies the UpdateMask to merge the changes into a new LinearizedObject.
func Merge(mask *UpdateMask, current LinearizedObject, diff LinearizedObject) (LinearizedObject, error) {
	// Create a new object to hold the merged result
	mergedObject := make(LinearizedObject)

	// Copy existing object to the merged result
	for key, value := range current {
		mergedObject[key] = value
	}

	// Iterate over the mask values and apply the updates
	for key, maskValue := range mask.Values {
		// Cast the key to int32 to match the map type
		intKey := int32(key)

		if nestedMask := maskValue.GetMultiple(); nestedMask != nil {
			// Handle the case where there are multiple values (nested)
			if existingVal, ok := current[intKey]; ok {
				// Case 1: If the existing value is a LinearizedMap, merge it as a map
				if existingMap, ok := existingVal.(LinearizedMap); ok {
					if updateMap, ok := diff[intKey].(LinearizedMap); ok {
						// Merge nested maps recursively
						mergedMap, err := mergeMaps(nestedMask, existingMap, updateMap)
						if err != nil {
							return nil, err
						}
						mergedObject[intKey] = mergedMap
					} else {
						// If the update value is not a LinearizedMap, return an error
						return nil, fmt.Errorf("expected LinearizedMap at key %v", key)
					}
				} else if existingArray, ok := existingVal.(LinearizedArray); ok {
					// Case 2: If the existing value is a LinearizedArray, merge it as an array
					if updateArray, ok := diff[intKey].(LinearizedArray); ok {
						// Merge nested arrays recursively
						mergedArray, err := mergeArrays(nestedMask, existingArray, updateArray)
						if err != nil {
							return nil, err
						}
						mergedObject[intKey] = mergedArray
					} else {
						// If the update value is not a LinearizedArray, return an error
						return nil, fmt.Errorf("expected LinearizedArray at key %v", key)
					}
				} else {
					// If the existing value is neither a map nor array, return an error
					return nil, fmt.Errorf("expected LinearizedMap or LinearizedArray at key %v", key)
				}
			}
		} else {
			// If there is no value in the existing object, just apply the update
			if updateVal, ok := diff[intKey]; ok {
				mergedObject[intKey] = updateVal
			}
		}

	}

	// Return the merged object
	return mergedObject, nil
}

// Helper function to merge LinearizedArray
func mergeArrays(mask *UpdateMask, current LinearizedArray, diff LinearizedArray) (LinearizedArray, error) {
	// Create a new array for the merged result
	mergedArray := make(LinearizedArray, len(current))

	// Copy existingArray into mergedArray
	copy(mergedArray, current)

	// Iterate over the mask values and apply the updates
	for index, maskValue := range mask.Values {
		// Determine the array position to modify
		pos := int(index)

		// Ensure the position is valid or append new items
		if pos >= len(mergedArray) {
			// If the position is beyond the current size, append empty slots until valid
			for len(mergedArray) <= pos {
				mergedArray = append(mergedArray, nil)
			}
		}
		nestedMask := maskValue.GetMultiple()
		if nestedMask != nil {

			switch existingItem := mergedArray[pos].(type) {
			case LinearizedObject:
				// If the existing value is a LinearizedObject, recursively merge
				if updateItem, ok := diff[pos].(LinearizedObject); ok {
					mergedItem, err := Merge(nestedMask, existingItem, updateItem)
					if err != nil {
						return nil, err
					}
					mergedArray[pos] = mergedItem
				} else {
					return nil, fmt.Errorf("expected LinearizedObject at position %d", pos)
				}

			case LinearizedArray:
				// If the existing value is a LinearizedArray, recursively merge arrays
				if updateItem, ok := diff[pos].(LinearizedArray); ok {
					mergedItem, err := mergeArrays(nestedMask, existingItem, updateItem)
					if err != nil {
						return nil, err
					}
					mergedArray[pos] = mergedItem
				} else {
					return nil, fmt.Errorf("expected LinearizedArray at position %d", pos)
				}

			case LinearizedMap:
				// If the existing value is a LinearizedMap, recursively merge maps
				if updateItem, ok := diff[pos].(LinearizedMap); ok {
					mergedItem, err := mergeMaps(nestedMask, existingItem, updateItem)
					if err != nil {
						return nil, err
					}
					mergedArray[pos] = mergedItem
				} else {
					return nil, fmt.Errorf("expected LinearizedMap at position %d", pos)
				}

			default:
				return nil, fmt.Errorf("unexpected type at position %d", pos)
			}
		} else {
			// Apply the single value update
			if updateVal := diff[pos]; updateVal != nil {
				mergedArray[pos] = updateVal
			}
		}

	}

	// Return the merged array
	return mergedArray, nil
}

// Helper function to merge LinearizedMap
func mergeMaps(mask *UpdateMask, current LinearizedMap, diff LinearizedMap) (LinearizedMap, error) {
	// Create a map to track keys and merge results
	keyIndex := make(map[int32]int) // Map from key to index in existingMap
	for i, pair := range current {
		if len(pair) == 2 {
			if key, ok := pair[0].(int32); ok {
				keyIndex[key] = i
			}
		}
	}

	// Copy the existing map into the merged result
	mergedMap := make(LinearizedMap, len(current))
	copy(mergedMap, current)

	// Iterate over the mask and apply updates
	for key, maskValue := range mask.Values {
		intKey := int32(key) // Convert key to int32

		// Find the index of the key in the existing map
		index, exists := keyIndex[intKey]

		// Handle single value updates
		if maskValue.GetEmpty() != nil {
			// Look for the updated value in the updateMap
			var updateVal interface{}
			for _, pair := range diff {
				if len(pair) == 2 && pair[0] == intKey {
					updateVal = pair[1]
					break
				}
			}

			// Update the existing map
			if exists {
				mergedMap[index][1] = updateVal
			} else {
				// Add a new key-value pair if the key doesn't exist
				mergedMap = append(mergedMap, [2]interface{}{intKey, updateVal})
			}
		} else if nestedMask := maskValue.GetMultiple(); nestedMask != nil {
			// Handle nested updates recursively
			var existingVal, updateVal interface{}
			if exists {
				existingVal = mergedMap[index][1]
			}
			for _, pair := range diff {
				if len(pair) == 2 && pair[0] == intKey {
					updateVal = pair[1]
					break
				}
			}

			switch existingItem := existingVal.(type) {
			case LinearizedObject:
				if updateItem, ok := updateVal.(LinearizedObject); ok {
					mergedItem, err := Merge(nestedMask, existingItem, updateItem)
					if err != nil {
						return nil, err
					}
					if exists {
						mergedMap[index][1] = mergedItem
					} else {
						mergedMap = append(mergedMap, [2]interface{}{intKey, mergedItem})
					}
				} else {
					return nil, fmt.Errorf("expected LinearizedObject for key %d", intKey)
				}
			case LinearizedArray:
				if updateItem, ok := updateVal.(LinearizedArray); ok {
					mergedItem, err := mergeArrays(nestedMask, existingItem, updateItem)
					if err != nil {
						return nil, err
					}
					if exists {
						mergedMap[index][1] = mergedItem
					} else {
						mergedMap = append(mergedMap, [2]interface{}{intKey, mergedItem})
					}
				} else {
					return nil, fmt.Errorf("expected LinearizedArray for key %d", intKey)
				}
			default:
				// Update primitive values
				if updateVal != nil {
					if exists {
						mergedMap[index][1] = updateVal
					} else {
						mergedMap = append(mergedMap, [2]interface{}{intKey, updateVal})
					}
				}
			}
		}
	}

	return mergedMap, nil
}
