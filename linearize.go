package linearize

import (
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// LinearizedObject is a map of integer keys to any values (used for flattened Protobuf objects)
type LinearizedObject map[int]any

// LinearizedArray is a slice of any values (used for Protobuf repeated fields)
type LinearizedArray []any

// LinearizedMap is a map of any keys to any values (used for Protobuf map fields)
type LinearizedMap map[any]any

// Linearize recursively flattens a Protobuf message into a LinearizedObject.
func Linearize(message proto.Message) (LinearizedObject, error) {
	linearized := make(LinearizedObject)

	// Return an empty LinearizedObject for nil message
	if message == nil {
		return linearized, nil
	}

	// Use reflection to inspect the message fields
	msgReflect := message.ProtoReflect()

	// Iterate over the fields of the message
	msgReflect.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		key := int(fd.Number()) // Use field number as the key

		// Handle map fields
		if fd.IsMap() {
			mapValue := make(LinearizedMap)
			// Sorting function that uses CRC32 hash values for non-sortable keys
			value.Map().Range(func(mapKey protoreflect.MapKey, mapVal protoreflect.Value) bool {
				// Create a slice to hold the keys and their corresponding CRC32 hash
				type keyedValue struct {
					Key   protoreflect.MapKey
					CRC32 uint32
				}

				var keys []keyedValue

				// Collect keys and compute CRC32 for each key
				value.Map().Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
					hash := crc32.ChecksumIEEE([]byte(k.String()))
					keys = append(keys, keyedValue{Key: k, CRC32: hash})
					return true
				})

				// Sort the keys based on the CRC32 hash value
				sort.Slice(keys, func(i, j int) bool {
					return keys[i].CRC32 < keys[j].CRC32
				})

				// After sorting, extract the map values in sorted order
				mapValue := make(LinearizedMap)
				for _, kv := range keys {
					mapValue[kv.Key.Interface()] = value.Map().Get(kv.Key).Interface()
				}

				// Return true to continue iteration (if needed)
				return true
			})
			linearized[key] = mapValue
		} else if fd.IsList() {
			// Handle repeated fields (lists)
			var list LinearizedArray
			for i := 0; i < value.List().Len(); i++ {
				list = append(list, value.List().Get(i).Interface())
			}
			linearized[key] = list
		} else if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			// Recursively handle nested messages
			// Convert protoreflect.Message to proto.Message by calling its Interface() method
			nestedMessage := value.Message().Interface()

			// Now you can pass the nestedMessage to Linearize
			nestedResult, err := Linearize(nestedMessage.(proto.Message))
			if err != nil {
				return false
			}
			linearized[key] = nestedResult
		} else {
			// Handle primitive fields
			linearized[key] = value.Interface()
		}
		return true
	})

	return linearized, nil
}

// Diff compares two LinearizedObject maps and returns before, after, and mask
func Diff(previous, latest LinearizedObject) (before LinearizedObject, after LinearizedObject, mask []*UpdateMask, err error) {
	before = make(LinearizedObject)
	after = make(LinearizedObject)
	mask = []*UpdateMask{}

	for key, prevValue := range previous {
		latestValue, exists := latest[key]
		if !exists {
			// If key is removed
			before[key] = prevValue
			after[key] = nil
			mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(key)}})
			continue
		}

		// Track changes without DeepEqual, manually checking the types and differences
		if changed, nestedBefore, nestedAfter, nestedMask := compareValues(prevValue, latestValue); changed {
			before[key] = nestedBefore
			after[key] = nestedAfter
			mask = append(mask, nestedMask...)
		}
	}

	// Check for keys added in the latest version
	for key, latestValue := range latest {
		if _, exists := previous[key]; !exists {
			before[key] = nil
			after[key] = latestValue
			mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(key)}})
		}
	}

	return before, after, mask, nil
}

// compareValues compares two values and returns true if they are different
// along with the before, after, and the update mask.
func compareValues(prevValue, latestValue any) (bool, any, any, []*UpdateMask) {
	mask := []*UpdateMask{}
	if reflect.TypeOf(prevValue) == reflect.TypeOf(latestValue) {
		switch v := prevValue.(type) {
		case LinearizedObject:
			// Compare LinearizedObject
			nestedPrev, nestedLatest := v, latestValue.(LinearizedObject)
			nestedBefore, nestedAfter, nestedMask, err := Diff(nestedPrev, nestedLatest)
			if err != nil {
				return false, nil, nil, nil
			}
			if len(nestedMask) > 0 {
				return true, nestedBefore, nestedAfter, nestedMask
			}
		case LinearizedArray:
			// Compare LinearizedArray
			nestedPrev, nestedLatest := v, latestValue.(LinearizedArray)
			nestedBefore, nestedAfter, nestedMask, err := DiffArray(nestedPrev, nestedLatest)
			if err != nil {
				return false, nil, nil, nil
			}
			if len(nestedMask) > 0 {
				return true, nestedBefore, nestedAfter, nestedMask
			}
		case LinearizedMap:
			// Compare LinearizedMap
			nestedPrev, nestedLatest := v, latestValue.(LinearizedMap)
			nestedBefore, nestedAfter, nestedMask, err := DiffMap(nestedPrev, nestedLatest)
			if err != nil {
				return false, nil, nil, nil
			}
			if len(nestedMask) > 0 {
				return true, nestedBefore, nestedAfter, nestedMask
			}
		default:
			// For primitive types, just check the value
			if !reflect.DeepEqual(prevValue, latestValue) {
				mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(0)}}) // No specific key since this is a leaf node
				return true, prevValue, latestValue, mask
			}
		}
	}
	return false, nil, nil, nil
}

// DiffArray compares two LinearizedArray slices and returns before, after, and mask
func DiffArray(previous, latest LinearizedArray) (before LinearizedArray, after LinearizedArray, mask []*UpdateMask, err error) {
	before = LinearizedArray{}
	after = LinearizedArray{}
	mask = []*UpdateMask{}

	for i, prevValue := range previous {
		if i >= len(latest) {
			// If item is removed
			before = append(before, prevValue)
			after = append(after, nil)
			mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(i)}})
			continue
		}

		latestValue := latest[i]
		if changed, nestedBefore, nestedAfter, nestedMask := compareValues(prevValue, latestValue); changed {
			before = append(before, nestedBefore)
			after = append(after, nestedAfter)
			mask = append(mask, nestedMask...)
		}
	}

	// Check for items added in the latest version
	for i := len(previous); i < len(latest); i++ {
		after = append(after, latest[i])
		mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(i)}})
	}

	return before, after, mask, nil
}

// DiffMap compares two LinearizedMap maps and returns before, after, and mask
func DiffMap(previous, latest LinearizedMap) (before LinearizedMap, after LinearizedMap, mask []*UpdateMask, err error) {
	before = LinearizedMap{}
	after = LinearizedMap{}
	mask = []*UpdateMask{}

	for key, prevValue := range previous {
		latestValue, exists := latest[key]
		if !exists {
			// If key is removed
			before[key] = prevValue
			after[key] = nil
			mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(key.(int))}})
			continue
		}

		// Track changes without DeepEqual, manually checking the types and differences
		if changed, nestedBefore, nestedAfter, nestedMask := compareValues(prevValue, latestValue); changed {
			before[key] = nestedBefore
			after[key] = nestedAfter
			mask = append(mask, nestedMask...)
		}
	}

	// Check for keys added in the latest version
	for key, latestValue := range latest {
		if _, exists := previous[key]; !exists {
			before[key] = nil
			after[key] = latestValue
			mask = append(mask, &UpdateMask{Value: &UpdateMask_Single{Single: int32(key.(int))}})
		}
	}

	return before, after, mask, nil
}

// Merge applies the updateMask to the right data and merges the changes from left into the right.
func Merge(updateMask []*UpdateMask, previous LinearizedObject, latest LinearizedObject) LinearizedObject {
	merged := make(LinearizedObject)

	// Apply changes based on updateMask
	for _, mask := range updateMask {
		if single, ok := mask.Value.(*UpdateMask_Single); ok {
			merged[int(single.Single)] = latest[int(single.Single)]
		}
	}

	// Handle fields not in the update mask
	for key, value := range latest {
		if _, exists := merged[key]; !exists {
			merged[key] = value
		}
	}

	// Handle fields that exist in both previous and latest but were not in the mask
	for key, value := range previous {
		if _, exists := merged[key]; !exists {
			merged[key] = value
		}
	}

	return merged
}

// Unlinearize function for decoding LinearizedObject into the correct struct type
func Unlinearize[T proto.Message](m LinearizedObject) (T, error) {
	var result T

	// Ensure result is a pointer to a struct
	if reflect.TypeOf(result).Kind() != reflect.Ptr {
		return result, errors.New("result must be a pointer to a struct")
	}

	// Initialize the result as a pointer if it's nil
	if reflect.ValueOf(result).IsNil() {
		// Create a new instance of the struct T (a pointer to it)
		result = reflect.New(reflect.TypeOf(result).Elem()).Interface().(T)
	}

	// Recursively unlinearize the struct fields
	if err := unlinearizeStruct(reflect.ValueOf(result).Elem(), m); err != nil {
		return result, err
	}

	return result, nil
}

func unlinearizeStruct(v reflect.Value, data LinearizedObject) error {
	// Iterate through the LinearizedObject by position and set the corresponding fields in the struct
	for i, d := range data {
		// Get the struct field at the given position
		if i >= v.NumField() {
			return fmt.Errorf("invalid position %d: exceeds struct field count", i)
		}

		// Get the field by position (using v.Field(pos))
		field := v.Field(i)
		if !field.IsValid() {
			return fmt.Errorf("invalid field for position: %d", i)
		}

		// Ensure the field is addressable (can be set)
		if !field.CanSet() {
			return fmt.Errorf("field at position %d cannot be set", i)
		}

		// Handle the field depending on its type
		switch value := d.(type) {
		case LinearizedObject:
			// Handle nested struct (LinearizedObject is a map of field numbers)
			// do I need to do dereference the pointer?
			// if field.Kind() != reflect.Struct {
			// 	return fmt.Errorf("expected struct field at position %d, but got %s", i, field.Kind())
			// }
			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}
				field = field.Elem()
			}
			if err := unlinearizeStruct(field, value); err != nil {
				return err
			}

		case LinearizedArray:
			// Handle repeated fields (slices)
			if field.Kind() != reflect.Slice {
				return fmt.Errorf("expected slice field at position %d, but got %s", i, field.Kind())
			}
			if field.IsNil() {
				field.Set(reflect.MakeSlice(field.Type(), len(value), len(value)))
			}
			sliceValue := field
			for i, elem := range value {
				sliceValue.Index(i).Set(reflect.ValueOf(elem))
			}

		case LinearizedMap:
			// Handle map fields
			if field.Kind() != reflect.Map {
				return fmt.Errorf("expected map field at position %d, but got %s", i, field.Kind())
			}
			// Initialize the map if it's a pointer and nil
			if field.Kind() == reflect.Ptr && field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			mapValue := field
			for mapKey, mapVal := range value {
				mapValue.SetMapIndex(reflect.ValueOf(mapKey), reflect.ValueOf(mapVal))
			}

		default:
			// Handle primitive fields
			if field.Kind() != reflect.TypeOf(value).Kind() {
				return fmt.Errorf("expected %s field at position %d, but got %s", reflect.TypeOf(value).Kind(), i, field.Kind())
			}
			field.Set(reflect.ValueOf(value))
		}
	}

	return nil
}
