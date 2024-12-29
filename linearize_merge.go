package linearize

// Merge applies the UpdateMask operations (ADD, UPDATE, REMOVE) to the current LinearizedObject
// directly modifying it using the diff and the UpdateMask.
func Merge(mask *UpdateMask, current LinearizedObject, diff LinearizedObject) error {
	// Apply operations based on the mask
	for pos, maskValue := range mask.Values {
		switch maskValue.Op {
		case UpdateMaskOperation_ADD, UpdateMaskOperation_UPDATE:

			// If there's a nested mask, merge recursively for nested structures
			if maskValue.Masks != nil {
				if nestedVal, exists := current[pos]; exists {
					// Handle nested structures: LinearizedObject, LinearizedSlice, LinearizedMap
					switch nestedVal := nestedVal.(type) {
					case LinearizedObject:
						// Recursively merge LinearizedObjects
						err := Merge(maskValue.Masks, nestedVal, diff[pos].(LinearizedObject))
						if err != nil {
							return err
						}
					case LinearizedSlice:
						// Handle merging of LinearizedSlice (slices)
						err := mergeSlices(maskValue.Masks, nestedVal, diff[pos].(LinearizedSlice))
						if err != nil {
							return err
						}
					case LinearizedMap:
						// Handle merging of LinearizedMap
						err := mergeMaps(maskValue.Masks, nestedVal, diff[pos].(LinearizedMap))
						if err != nil {
							return err
						}
					}
				}
			} else {
				if diffVal, exists := diff[pos]; exists {
					// Update the current object with the value from the diff
					current[pos] = diffVal
				}
			}

		case UpdateMaskOperation_REMOVE:
			// For REMOVE, delete the key from the current object
			delete(current, pos)
		}
	}

	return nil
}

// mergeSlices merges two LinearizedSlice types using the update mask
func mergeSlices(mask *UpdateMask, current, diff LinearizedSlice) error {
	// Apply operations based on the mask
	for pos, maskValue := range mask.Values {
		switch maskValue.Op {
		case UpdateMaskOperation_ADD:
			// For ADD, apply the diff if it exists
			if diffVal, exists := diff[pos]; exists {
				current[pos] = diffVal
			}
		case UpdateMaskOperation_REMOVE:
			// For REMOVE, delete the value at the specified position
			delete(current, pos)
		case UpdateMaskOperation_UPDATE:
			// For UPDATE, apply the diff if it exists
			if diffVal, exists := diff[pos]; exists {
				current[pos] = diffVal
			}
		}
	}
	return nil
}

// mergeMaps merges two LinearizedMap types using the update mask
func mergeMaps(mask *UpdateMask, current, diff LinearizedMap) error {
	// Apply operations based on the mask
	for pos, maskValue := range mask.Values {
		switch maskValue.Op {
		case UpdateMaskOperation_ADD:
			// For ADD, apply the diff if it exists
			if diffVal, exists := diff[pos]; exists {
				current[pos] = diffVal
			}
		case UpdateMaskOperation_REMOVE:
			// For REMOVE, delete the key from the current map
			delete(current, pos)
		case UpdateMaskOperation_UPDATE:
			// For UPDATE, apply the diff if it exists
			if diffVal, exists := diff[pos]; exists {
				current[pos] = diffVal
			}
		}
	}
	return nil
}
