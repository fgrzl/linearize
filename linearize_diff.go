package linearize

// Diff compares two LinearizedObject maps and returns before, after, and a single mask.
func Diff(previous, latest LinearizedObject) (before LinearizedObject, after LinearizedObject, mask *UpdateMask, err error) {
	before = make(LinearizedObject)
	after = make(LinearizedObject)
	masks := make(map[int32]*UpdateMaskValue) // Map of masks for each key

	// Iterate over the previous map to find removed or changed keys
	for key, prevValue := range previous {
		pos := int32(key)
		latestValue, exists := latest[key]
		if !exists {
			// If key is removed, mark it for removal in the mask
			before[key] = prevValue
			after[key] = nil
			masks[pos] = &UpdateMaskValue{Op: UpdateMaskOperation_REMOVE}
			continue
		}

		changed, nestedBefore, nestedAfter, nestedMask := compareValues(prevValue, latestValue)
		if changed {
			// If there is a change, add the before/after values and the nested mask (if present)
			before[key] = nestedBefore
			after[key] = nestedAfter
			if nestedMask != nil {
				// Handle nested mask (it will be a nested UpdateMask)
				masks[pos] = &UpdateMaskValue{Op: UpdateMaskOperation_UPDATE, Masks: nestedMask}
			} else {
				masks[pos] = &UpdateMaskValue{Op: UpdateMaskOperation_UPDATE}
			}
		}
	}

	// Check for keys added in the latest version that are not in the previous version
	for key, latestValue := range latest {
		pos := int32(key)
		if _, exists := previous[key]; !exists {
			before[key] = nil
			after[key] = latestValue
			masks[pos] = &UpdateMaskValue{Op: UpdateMaskOperation_ADD}
		}
	}

	// Convert the map to the final UpdateMask
	if len(masks) > 0 {
		mask = &UpdateMask{Values: masks} // Assign the map of masks to the Values field
		return before, after, mask, nil
	}

	// If no differences are found, return nil for all values
	return nil, nil, nil, nil
}

// compareValues compares two values and returns if they have changed and the mask.
func compareValues(prevValue, latestValue any) (changed bool, nestedBefore, nestedAfter any, nestedMask *UpdateMask) {
	// Initialize a new UpdateMask
	nestedMask = &UpdateMask{Values: make(map[int32]*UpdateMaskValue)}

	// Handle complex types
	switch prev := prevValue.(type) {
	case LinearizedObject:
		if latest, ok := latestValue.(LinearizedObject); ok {
			// Recursively compare objects
			changed = false
			nestedBefore = make(LinearizedObject) // Initialize a new LinearizedObject for before state
			nestedAfter = make(LinearizedObject)  // Initialize a new LinearizedObject for after state

			// Compare keys that are in both objects
			for key, prevVal := range prev {
				latestVal, exists := latest[key]
				if !exists {
					// Key was removed in the latest object
					nestedBefore.(LinearizedObject)[key] = prevVal
					nestedAfter.(LinearizedObject)[key] = nil
					nestedMask.Values[int32(key)] = &UpdateMaskValue{Op: UpdateMaskOperation_REMOVE}
					changed = true
					continue
				}

				// Compare values recursively
				elemChanged, elemBefore, elemAfter, elemMask := compareValues(prevVal, latestVal)
				if elemChanged {
					// Update nestedBefore and nestedAfter with the changed values for this key
					nestedBefore.(LinearizedObject)[key] = elemBefore
					nestedAfter.(LinearizedObject)[key] = elemAfter
					changed = true
					// Store the nested mask for this key
					if elemMask != nil {
						nestedMask.Values[int32(key)] = &UpdateMaskValue{
							Op:    UpdateMaskOperation_UPDATE,
							Masks: elemMask,
						}
					} else {
						nestedMask.Values[int32(key)] = &UpdateMaskValue{Op: UpdateMaskOperation_UPDATE}
					}
				} else {
					// If no change, keep the existing values
					nestedBefore.(LinearizedObject)[key] = prevVal
					nestedAfter.(LinearizedObject)[key] = latestVal
				}
			}

			// Check for new keys in the latest object
			for key, latestVal := range latest {
				if _, exists := prev[key]; !exists {
					// If key is new, add it to the after state with an empty before state
					nestedBefore.(LinearizedObject)[key] = nil
					nestedAfter.(LinearizedObject)[key] = latestVal
					nestedMask.Values[int32(key)] = &UpdateMaskValue{Op: UpdateMaskOperation_ADD}
					changed = true
				}
			}

			// Return the result for the object comparison
			return changed, nestedBefore, nestedAfter, nestedMask
		}

	case LinearizedSlice:
		if latest, ok := latestValue.(LinearizedSlice); ok {
			// Compare arrays element by element
			changed = false
			maxLen := max(len(prev), len(latest))
			mergedBefore := make(LinearizedSlice, maxLen)
			mergedAfter := make(LinearizedSlice, maxLen)

			for i := 0; i < maxLen; i++ {
				key := int32(i)
				var prevElem, latestElem any
				if i < len(prev) {
					prevElem = prev[key]
				}
				if i < len(latest) {
					latestElem = latest[key]
				}

				// Compare elements
				elemChanged, elemBefore, elemAfter, elemMask := compareValues(prevElem, latestElem)
				mergedBefore[key] = elemBefore
				mergedAfter[key] = elemAfter
				if elemChanged {
					changed = true
					nestedMask.Values[int32(key)] = &UpdateMaskValue{
						Op:    UpdateMaskOperation_UPDATE,
						Masks: elemMask,
					}
				}
			}
			return changed, mergedBefore, mergedAfter, nestedMask
		}

	case LinearizedMap:
		if latest, ok := latestValue.(LinearizedMap); ok {
			// Compare maps key by key
			changed = false
			keySet := make(map[any]struct{})

			// Add all keys from both previous and latest map
			for _, pair := range prev {
				keySet[pair[0]] = struct{}{}
			}
			for _, pair := range latest {
				keySet[pair[0]] = struct{}{}
			}

			mergedBefore := make([][2]any, 0)
			mergedAfter := make([][2]any, 0)

			// Iterate over all keys
			for key := range keySet {
				var prevVal, latestVal any
				// Get the previous value for the current key
				for _, pair := range prev {
					if pair[0] == key {
						prevVal = pair[1]
						mergedBefore = append(mergedBefore, pair)
						break
					}
				}
				// Get the latest value for the current key
				for _, pair := range latest {
					if pair[0] == key {
						latestVal = pair[1]
						mergedAfter = append(mergedAfter, pair)
						break
					}
				}

				// Compare values
				elemChanged, elemBefore, elemAfter, elemMask := compareValues(prevVal, latestVal)
				if elemChanged {
					changed = true
					// Store the before and after states correctly
					mergedBefore[len(mergedBefore)-1] = [2]any{key, elemBefore}
					mergedAfter[len(mergedAfter)-1] = [2]any{key, elemAfter}
					if elemMask != nil {
						nestedMask.Values[int32(key.(int))] = &UpdateMaskValue{
							Op:    UpdateMaskOperation_UPDATE,
							Masks: elemMask,
						}
					}
				}
			}
			return changed, mergedBefore, mergedAfter, nestedMask
		}

	default:
		// Handle primitive values directly
		if prevValue != latestValue {
			return true, prevValue, latestValue, nil
		}
	}

	// No changes detected
	return false, nil, nil, nil
}

// Helper function to calculate max of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
