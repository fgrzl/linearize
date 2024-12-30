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

			changed = false
			prevLen := len(prev)
			latestLen := len(latest)

			maxLen := max(prevLen, latestLen)
			mergedBefore := make(LinearizedSlice, maxLen)
			mergedAfter := make(LinearizedSlice, maxLen)

			for i := 0; i < maxLen; i++ {
				key := int32(i)

				var prevElem, latestElem any
				if i < prevLen {
					prevElem = prev[key]
				}
				if i < latestLen {
					latestElem = latest[key]
				}

				// Compare elements
				elemChanged, elemBefore, elemAfter, elemMask := compareValues(prevElem, latestElem)
				mergedBefore[key] = elemBefore
				if elemChanged {
					changed = true

					if latestLen < prevLen {
						if i >= prevLen-1 {
							changed = true
							nestedMask.Values[int32(key)] = &UpdateMaskValue{
								Op: UpdateMaskOperation_REMOVE,
							}
							continue
						}
					}
					mergedAfter[key] = elemAfter
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
			changed := false
			mergedBefore := make(LinearizedMap)
			mergedAfter := make(LinearizedMap)

			// Check keys in the previous map (prev) and compare with the latest map
			for key, prevVal := range prev {

				// Check if the key is present in the latest map
				latestVal, exists := latest[key]
				if !exists {
					// If key is removed in the latest map, mark for removal
					mergedBefore[key] = prevVal
					mergedAfter[key] = [2]any{}
					nestedMask.Values[key] = &UpdateMaskValue{
						Op: UpdateMaskOperation_REMOVE,
					}
					changed = true
					continue
				}

				// If key is present in both maps, compare the values
				elemChanged, elemBefore, elemAfter, elemMask := compareValues(prevVal, latestVal)

				// Cast elemBefore and elemAfter to [2]any
				if elemBefore != nil {
					mergedBefore[key] = elemBefore.([2]any)
				}
				if elemAfter != nil {
					mergedAfter[key] = elemAfter.([2]any)
				}

				if elemChanged {
					changed = true
					if elemMask != nil {
						nestedMask.Values[key] = &UpdateMaskValue{
							Op:    UpdateMaskOperation_UPDATE,
							Masks: elemMask,
						}
					} else {
						nestedMask.Values[key] = &UpdateMaskValue{
							Op: UpdateMaskOperation_UPDATE,
						}
					}
				} else {
					// If no change, retain the value
					mergedAfter[key] = latestVal
				}
			}

			// Check for new keys in the latest map
			for key, latestVal := range latest {

				if _, exists := prev[key]; !exists {
					// If key is new, mark for addition
					mergedBefore[key] = [2]any{}
					mergedAfter[key] = latestVal
					nestedMask.Values[key] = &UpdateMaskValue{
						Op: UpdateMaskOperation_ADD,
					}
					changed = true
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
